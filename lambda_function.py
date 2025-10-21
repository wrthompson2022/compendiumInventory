import os
import json
import time
import io
import csv
import uuid
import base64
import logging
import datetime as dt
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple, Optional

import psycopg2
import psycopg2.extras
import boto3
import requests
from requests.adapters import HTTPAdapter, Retry
import paramiko

# ----------------------------
# Logging
# ----------------------------
log = logging.getLogger()
log.setLevel(logging.INFO)

# ----------------------------
# Config helpers (Secrets Manager)
# ----------------------------
def get_secret_json(secret_name: str, region: Optional[str] = None) -> dict:
    sm = boto3.client("secretsmanager", region_name=region)
    val = sm.get_secret_value(SecretId=secret_name)
    s = val.get("SecretString") or base64.b64decode(val["SecretBinary"]).decode("utf-8")
    return json.loads(s)

def get_db_conn():
    secret_name = os.environ.get("DB_SECRET_NAME")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    cfg = get_secret_json(secret_name, region)
    return psycopg2.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 5432)),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=10,
        sslmode="require",
    )

def get_cin7_auth():
    # Secret structure provided by you:
    # {"cin7":"{ \"nz\": {\"username\":\"...\",\"password\":\"...\"}, \"aus\": { ... } }"}
    secret_name = os.environ.get("CIN7_SECRET_NAME")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    raw = get_secret_json(secret_name, region)
    inner = raw.get("cin7")
    auth_config = json.loads(inner) if isinstance(inner, str) else inner
    return {
        "nz": (auth_config["nz"]["username"], auth_config["nz"]["password"]),
        "aus": (auth_config["aus"]["username"], auth_config["aus"]["password"]),
    }

def get_sftp_cfg():
    # Expect {"host","port","username","password"}
    secret_name = os.environ.get("SFTP_SECRET_NAME")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    return get_secret_json(secret_name, region)

# ----------------------------
# HTTP w/ backoff (Cin7)
# ----------------------------
class Cin7Session(requests.Session):
    BASE = "https://api.cin7.com/api/v1/"

    def __init__(self, username: str, password: str, label: str):
        super().__init__()
        self.auth = (username, password)
        self.label = label
        retries = Retry(
            total=8,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_maxsize=16)
        self.mount("https://", adapter)
        self.headers.update({"Accept": "application/json"})

    def _backoff(self, url: str, params: dict = None):
        sleep = 1.0
        while True:
            r = self.get(url, params=params, timeout=30)
            if r.status_code == 429:
                log.warning("429 from server. Sleeping %.1fs", sleep)
                time.sleep(sleep)
                sleep = min(sleep * 1.5, 6.0)
                continue
            r.raise_for_status()
            return r

    def get_pages(self, resource: str, params: dict, max_pages: int = 200):
        last_text = None
        for page in range(1, max_pages + 1):
            qp = dict(params)
            qp["page"] = page
            qp["rows"] = 250  # Cin7 API supports up to 250 rows per page
            url = self.BASE + resource
            r = self._backoff(url, qp)
            if not r.text or r.text == last_text:
                if page > 1:
                    log.warning("[%s] %s page %s: identical/empty, stopping.", self.label, resource, page)
                break
            data = r.json() or []
            log.info("[%s] %s page %d: %d rows", self.label, resource, page, len(data))
            if not data:
                break
            yield data
            last_text = r.text

# ----------------------------
# Pulls
# ----------------------------
FIELDS_PRODUCTS = "id,code,styleCode,status,createdDate,productOptions(id,code,productOptionCode)"
FIELDS_STOCK = "branchId,code,productOptionCode,stockOnHand,stockAllocated,stockAvailable,updatedDate"
FIELDS_SALES = (
    "id,reference,status,isApproved,memberId,branchId,createdDate,estimatedDeliveryDate,"
    "lineItems(id,code,qtyShipped,qty)"
)
FIELDS_PURCHASE = (
    "id,reference,status,isApproved,supplierId,branchId,createdDate,estimatedArrivalDate,"
    "lineItems(id,code,qtyReceived,qty)"
)

def pull_products(conn, team: str, run_id: uuid.UUID, sess: Cin7Session,
                  created_lb: Optional[str], style_filter: Optional[str]) -> set:
    params = {"fields": FIELDS_PRODUCTS, "where": "status<>'Disabled'"}
    if created_lb:
        params["where"] += f" and createdDate>='{created_lb}'"
    if style_filter:
        params["where"] += f" and styleCode='{style_filter}'"

    option_codes = set()
    batch = []
    BATCH_SIZE = 1000

    with conn, conn.cursor() as cur:
        psycopg2.extras.register_uuid()
        for page in sess.get_pages("Products", params):
            for p in page:
                style_code = p.get("styleCode")
                for opt in p.get("productOptions") or []:
                    option_code = opt.get("code")
                    if not option_code:
                        continue
                    option_codes.add(option_code)
                    batch.append((
                        team, run_id, sess.label, p.get("id"), p.get("code"),
                        style_code, opt.get("id"), option_code, style_code,
                        bool(p.get("isActive", True))
                    ))

                    if len(batch) >= BATCH_SIZE:
                        psycopg2.extras.execute_batch(cur,
                            """
                            INSERT INTO cin7_product_snapshot
                            (team_name, run_id, instance, product_id, product_code, style_code,
                             option_id, option_code, option_style_code, is_active, eligible)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,False)
                            ON CONFLICT (team_name, run_id, instance, option_code) DO NOTHING
                            """, batch, page_size=BATCH_SIZE)
                        batch = []

        # Insert remaining
        if batch:
            psycopg2.extras.execute_batch(cur,
                """
                INSERT INTO cin7_product_snapshot
                (team_name, run_id, instance, product_id, product_code, style_code,
                 option_id, option_code, option_style_code, is_active, eligible)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,False)
                ON CONFLICT (team_name, run_id, instance, option_code) DO NOTHING
                """, batch, page_size=len(batch))

        # Mark as eligible for this run (you can refine via your mapping job)
        cur.execute(
            """UPDATE cin7_product_snapshot
                  SET eligible = True
                WHERE team_name=%s AND run_id=%s AND instance=%s""",
            (team, run_id, sess.label),
        )
        cur.execute(
            """SELECT option_code FROM cin7_product_snapshot
               WHERE team_name=%s AND run_id=%s AND instance=%s AND eligible=True""",
            (team, run_id, sess.label),
        )
        elig = {r[0] for r in cur.fetchall()}
    log.info("[%s] Product options snapped (eligible): %d", sess.label, len(elig))
    return elig

def pull_stock(conn, team: str, run_id: uuid.UUID, sess: Cin7Session,
               branch_ids: List[int], eligible: set):
    BATCH_SIZE = 1000
    with conn, conn.cursor() as cur:
        for bid in branch_ids:
            params = {"fields": FIELDS_STOCK, "where": f"branchId={bid}"}
            batch = []
            total = 0
            for page in sess.get_pages("Stock", params, max_pages=300):
                for s in page:
                    sku = s.get("code")
                    if not sku or (eligible and sku not in eligible):
                        continue
                    on_hand = Decimal(str(s.get("stockOnHand") or 0))
                    alloc = Decimal(str(s.get("stockAllocated") or 0))
                    avail = Decimal(str(s.get("stockAvailable") or 0))
                    updated = s.get("updatedDate")
                    batch.append((
                        team, run_id, sess.label, bid, sku, s.get("productOptionCode"),
                        on_hand, alloc, avail,
                        dt.datetime.fromisoformat(updated.replace("Z","+00:00")) if updated else dt.datetime.utcnow()
                    ))
                    total += 1

                    if len(batch) >= BATCH_SIZE:
                        psycopg2.extras.execute_batch(cur,
                            """
                            INSERT INTO cin7_stock_snapshot
                            (team_name, run_id, instance, branch_id, sku, product_option,
                             on_hand, allocated, available, updated_at_utc)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (team_name, run_id, instance, branch_id, sku) DO UPDATE
                               SET on_hand=EXCLUDED.on_hand, allocated=EXCLUDED.allocated,
                                   available=EXCLUDED.available, updated_at_utc=EXCLUDED.updated_at_utc
                            """, batch, page_size=BATCH_SIZE)
                        batch = []

            # Insert remaining
            if batch:
                psycopg2.extras.execute_batch(cur,
                    """
                    INSERT INTO cin7_stock_snapshot
                    (team_name, run_id, instance, branch_id, sku, product_option,
                     on_hand, allocated, available, updated_at_utc)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_name, run_id, instance, branch_id, sku) DO UPDATE
                       SET on_hand=EXCLUDED.on_hand, allocated=EXCLUDED.allocated,
                           available=EXCLUDED.available, updated_at_utc=EXCLUDED.updated_at_utc
                    """, batch, page_size=len(batch))

            log.info("[%s] Stock inserted for branch %s: %d rows", sess.label, bid, total)

def _choose(date_str: Optional[str], fallback: Optional[str]) -> dt.date:
    if date_str:
        try:
            return dt.date.fromisoformat(date_str[:10])
        except Exception:
            pass
    if fallback:
        try:
            return dt.date.fromisoformat(fallback[:10])
        except Exception:
            pass
    return dt.date.today()

def pull_sales_orders(conn, team: str, run_id: uuid.UUID, sess: Cin7Session,
                      branch_ids: List[int], eligible: set):
    BATCH_SIZE = 1000
    with conn, conn.cursor() as cur:
        for bid in branch_ids:
            where = f"status in ('Open','Draft','Approved') and branchId={bid}"
            params = {"fields": FIELDS_SALES, "where": where}
            batch = []
            inserted = 0
            for page in sess.get_pages("SalesOrders", params, max_pages=300):
                for so in page:
                    hdr_status = so.get("status")
                    is_approved = bool(so.get("isApproved"))
                    created = so.get("createdDate")
                    req_date = _choose(so.get("estimatedDeliveryDate"), created)
                    for li in so.get("lineItems") or []:
                        sku = li.get("code")
                        if not sku or (eligible and sku not in eligible):
                            continue
                        qty = Decimal(str(li.get("qty") or 0))
                        shipped = Decimal(str(li.get("qtyShipped") or 0))
                        # Calculate open demand as qty - qtyShipped (regardless of Draft/Open status)
                        demand = max(qty - shipped, Decimal("0"))
                        if demand == 0:
                            continue
                        batch.append((
                            team, run_id, sess.label, bid, so.get("reference") or str(so.get("id")),
                            f"{so.get('id')}-{li.get('id')}", sku, qty, shipped, demand, req_date,
                            so.get("memberId"), hdr_status, is_approved
                        ))
                        inserted += 1

                        if len(batch) >= BATCH_SIZE:
                            psycopg2.extras.execute_batch(cur,
                                """
                                INSERT INTO cin7_open_so
                                (team_name, run_id, instance, branch_id, so_number, so_line_id, sku,
                                 qty, qty_shipped, qty_demand, req_ship_date, customer_id, status, approved)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (team_name, run_id, instance, so_line_id) DO NOTHING
                                """, batch, page_size=BATCH_SIZE)
                            batch = []

            # Insert remaining
            if batch:
                psycopg2.extras.execute_batch(cur,
                    """
                    INSERT INTO cin7_open_so
                    (team_name, run_id, instance, branch_id, so_number, so_line_id, sku,
                     qty, qty_shipped, qty_demand, req_ship_date, customer_id, status, approved)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_name, run_id, instance, so_line_id) DO NOTHING
                    """, batch, page_size=len(batch))

            log.info("[%s] SO lines inserted for branch %s: %d", sess.label, bid, inserted)

def pull_purchase_orders(conn, team: str, run_id: uuid.UUID, sess: Cin7Session,
                         branch_ids: List[int], eligible: set):
    BATCH_SIZE = 1000
    with conn, conn.cursor() as cur:
        for bid in branch_ids:
            where = f"status in ('Open','Draft','Approved') and branchId={bid}"
            params = {"fields": FIELDS_PURCHASE, "where": where}
            batch = []
            inserted = 0
            for page in sess.get_pages("PurchaseOrders", params, max_pages=300):
                for po in page:
                    hdr_status = po.get("status")
                    created = po.get("createdDate")
                    eta = _choose(po.get("estimatedArrivalDate"), created)
                    for li in po.get("lineItems") or []:
                        sku = li.get("code")
                        if not sku or (eligible and sku not in eligible):
                            continue
                        qty = Decimal(str(li.get("qty") or 0))
                        recv = Decimal(str(li.get("qtyReceived") or 0))
                        # Calculate open quantity as qty - qtyReceived (regardless of Draft/Open status)
                        open_qty = max(qty - recv, Decimal("0"))
                        if open_qty == 0:
                            continue
                        batch.append((
                            team, run_id, sess.label, bid, po.get("reference") or str(po.get("id")),
                            f"{po.get('id')}-{li.get('id')}", sku, qty, recv, open_qty, eta,
                            str(po.get("supplierId") or ""), hdr_status
                        ))
                        inserted += 1

                        if len(batch) >= BATCH_SIZE:
                            psycopg2.extras.execute_batch(cur,
                                """
                                INSERT INTO cin7_open_po
                                (team_name, run_id, instance, branch_id, po_number, po_line_id, sku,
                                 qty, qty_received, qty_open, eta_date, supplier_code, status)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (team_name, run_id, instance, po_line_id) DO NOTHING
                                """, batch, page_size=BATCH_SIZE)
                            batch = []

            # Insert remaining
            if batch:
                psycopg2.extras.execute_batch(cur,
                    """
                    INSERT INTO cin7_open_po
                    (team_name, run_id, instance, branch_id, po_number, po_line_id, sku,
                     qty, qty_received, qty_open, eta_date, supplier_code, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_name, run_id, instance, po_line_id) DO NOTHING
                    """, batch, page_size=len(batch))

            log.info("[%s] PO lines inserted for branch %s: %d", sess.label, bid, inserted)

# ----------------------------
# ATP bucket math (reworked)
# ----------------------------
def compute_bucketed_atp(conn, team: str, run_id: uuid.UUID, instance: str):
    """
    Produce bucketed ATS (Available To Sell) with exactly these bucket dates per (branch, sku):
      - TODAY (always)
      - each PO eta_date in ascending order

    Demand assignment:
      - Bucket 0 (today): all SO demand with req_ship_date < first_eta (or all SOs if no POs)
      - Bucket i (eta_i): all SO demand with req_ship_date >= eta_{i-1} and < eta_i
      - Bucket last (eta_n): all SO demand with req_ship_date >= eta_n

    ATS Calculation (per Excel specification):
      For each bucket, ATS = IF(sum(demand[i:end]) > sum(supply[i:end]), 0, on_hand + receipts - demand)
      Where supply = current on_hand + all future receipts

      This ensures that if total remaining demand exceeds total remaining supply,
      ATS is set to 0 for that period (and typically all subsequent periods).
    """
    today = dt.date.today()

    with conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Keys to compute: any sku seen in stock/po/so for this instance/run
        cur.execute(
            """
            WITH keys AS (
              SELECT branch_id, sku FROM cin7_stock_snapshot
               WHERE team_name=%s AND run_id=%s AND instance=%s
              UNION
              SELECT branch_id, sku FROM cin7_open_po
               WHERE team_name=%s AND run_id=%s AND instance=%s
              UNION
              SELECT branch_id, sku FROM cin7_open_so
               WHERE team_name=%s AND run_id=%s AND instance=%s
            )
            SELECT DISTINCT branch_id, sku FROM keys
            """,
            (team, run_id, instance, team, run_id, instance, team, run_id, instance),
        )
        pairs = cur.fetchall()

        for row in pairs:
            branch_id, sku = row["branch_id"], row["sku"]

            # Opening = current available (can be 0)
            cur.execute(
                """
                SELECT available FROM cin7_stock_snapshot
                 WHERE team_name=%s AND run_id=%s AND instance=%s
                   AND branch_id=%s AND sku=%s
                """,
                (team, run_id, instance, branch_id, sku),
            )
            r = cur.fetchone()
            opening = Decimal(str(r[0])) if r else Decimal("0")

            # Receipts by ETA
            cur.execute(
                """
                SELECT eta_date, COALESCE(SUM(qty_open),0) AS qty
                  FROM cin7_open_po
                 WHERE team_name=%s AND run_id=%s AND instance=%s
                   AND branch_id=%s AND sku=%s
                 GROUP BY eta_date
                 ORDER BY eta_date
                """,
                (team, run_id, instance, branch_id, sku),
            )
            receipts_by_eta = [(rec["eta_date"], Decimal(str(rec["qty"] or 0))) for rec in cur.fetchall()]
            eta_list = [d for d, _ in receipts_by_eta if d is not None]

            # Demand by SO date
            cur.execute(
                """
                SELECT req_ship_date, COALESCE(SUM(qty_demand),0) AS qty
                  FROM cin7_open_so
                 WHERE team_name=%s AND run_id=%s AND instance=%s
                   AND branch_id=%s AND sku=%s
                 GROUP BY req_ship_date
                 ORDER BY req_ship_date
                """,
                (team, run_id, instance, branch_id, sku),
            )
            demand_by_date = [(rec["req_ship_date"], Decimal(str(rec["qty"] or 0))) for rec in cur.fetchall()]

            # Build bucket sequence: [today] + each ETA (unique, sorted)
            bucket_dates = [today] + eta_list

            # Helper to sum demand in a half-open interval [start, end)
            def sum_demand(start: Optional[dt.date], end: Optional[dt.date]) -> Decimal:
                total = Decimal("0")
                for d, q in demand_by_date:
                    if d is None:
                        continue
                    if (start is None or d >= start) and (end is None or d < end):
                        total += q
                return total

            # Prepare receipts dict (none on 'today')
            rec_map = {d: q for d, q in receipts_by_eta if d is not None}

            # Build list of (bucket_date, receipts, demand) for all buckets first
            bucket_data = []
            prev_cut = None
            for idx, bdate in enumerate(bucket_dates):
                next_cut = bucket_dates[idx + 1] if idx + 1 < len(bucket_dates) else None
                receipts = rec_map.get(bdate, Decimal("0")) if bdate != today else Decimal("0")

                if idx == 0:
                    demand = sum_demand(None, next_cut)
                else:
                    demand = sum_demand(prev_cut, next_cut)

                bucket_data.append((bdate, receipts, demand))
                prev_cut = bdate

            # Now calculate ATS for each bucket using forward-looking logic
            running = opening  # Track running balance for opening_balance field
            for idx, (bdate, receipts, demand) in enumerate(bucket_data):
                # Calculate total remaining supply from this bucket forward
                # Supply = current running balance (on-hand at this bucket) + all future receipts
                future_receipts = sum(r for _, r, _ in bucket_data[idx:])
                total_remaining_supply = running + future_receipts

                # Calculate total remaining demand from this bucket forward
                total_remaining_demand = sum(d for _, _, d in bucket_data[idx:])

                # ATS Formula: IF(total_remaining_demand > total_remaining_supply, 0, on_hand + receipts - demand)
                opening_balance = running
                if total_remaining_demand > total_remaining_supply:
                    ending_ats = Decimal("0")
                else:
                    ending_ats = opening_balance + receipts - demand

                # Upsert row
                cur.execute(
                    """
                    INSERT INTO cin7_atp_bucketed
                    (team_name, run_id, instance, branch_id, sku, bucket_date,
                     opening_balance, receipts_qty, demand_qty, ending_atp)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_name, run_id, instance, branch_id, sku, bucket_date) DO UPDATE
                       SET opening_balance=EXCLUDED.opening_balance,
                           receipts_qty=EXCLUDED.receipts_qty,
                           demand_qty=EXCLUDED.demand_qty,
                           ending_atp=EXCLUDED.ending_atp
                    """,
                    (team, run_id, instance, branch_id, sku, bdate,
                     opening_balance, receipts, demand, ending_ats),
                )

                # Update running balance for next bucket's opening
                running = running + receipts - demand

            # Edge case: no POs and no SOs — still ensure TODAY row exists
            if not eta_list and not demand_by_date:
                cur.execute(
                    """
                    INSERT INTO cin7_atp_bucketed
                    (team_name, run_id, instance, branch_id, sku, bucket_date,
                     opening_balance, receipts_qty, demand_qty, ending_atp)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_name, run_id, instance, branch_id, sku, bucket_date) DO NOTHING
                    """,
                    (team, run_id, instance, branch_id, sku, today,
                     opening, Decimal("0"), Decimal("0"), opening),
                )

# ----------------------------
# CSV + SFTP
# ----------------------------
def fetch_branch_name_map(conn, team: str, instance: str) -> Dict[int, str]:
    """
    Read warehouse names from your DB only.
    """
    out = {}
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT branch_id, branch_name FROM cin7_branch_map "
            "WHERE team_name=%s AND instance=%s AND is_active",
            (team, instance),
        )
        for bid, name in cur.fetchall():
            out[int(bid)] = name
    return out

def build_inventory_csv(conn, team: str, run_id: uuid.UUID, instance: str) -> List[Tuple[str,str,str,str]]:
    """
    CSV schema:
      WareHouse, StockItemKey, AvailableDate, Quantity
    WareHouse comes from cin7_branch_map.branch_name for the given instance+branch_id.
    Quantity is ATP after applying receipts and demand for that bucket (ending_atp).
    Always include a 'today' row (bucket_date == today) per (branch, sku).
    """
    id_to_name = fetch_branch_name_map(conn, team, instance)

    def bname(bid: int) -> str:
        return id_to_name.get(bid) or str(bid)

    rows = []
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.branch_id, a.sku, a.bucket_date, a.ending_atp
              FROM cin7_atp_bucketed a
             WHERE team_name=%s AND run_id=%s AND instance=%s
             ORDER BY a.branch_id, a.sku, a.bucket_date
            """,
            (team, run_id, instance),
        )
        for branch_id, sku, dte, qty in cur.fetchall():
            rows.append((bname(branch_id), sku, dte.isoformat(), str(qty)))
    return rows

def sftp_put(sftp_cfg: dict, local_bytes: bytes, remote_path: str):
    host = sftp_cfg["hostname"]
    port = int(sftp_cfg.get("port", 22))
    username = sftp_cfg["username"]
    password = sftp_cfg["password"]

    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    dir_path = os.path.dirname(remote_path).rstrip("/") or "/"
    try:
        sftp.chdir(dir_path)
    except IOError:
        parts = dir_path.strip("/").split("/")
        cur = ""
        for p in parts:
            cur = f"{cur}/{p}" if cur else f"/{p}"
            try:
                sftp.chdir(cur)
            except IOError:
                sftp.mkdir(cur)
                sftp.chdir(cur)

    with sftp.file(remote_path, "wb") as f:
        f.write(local_bytes)

    sftp.close()
    transport.close()

# ----------------------------
# Orchestrator
# ----------------------------
def run(event, context):
    team = event.get("team_name") or "Compendium"
    test_style = event.get("styleCode")  # optional style filter
    created_lb = event.get("created_since")  # optional ISO date lower bound for products
    branches_cfg = event.get("branches", {})  # {"nz":[...], "aus":[...]}

    log.info("== Run start ==")
    log.info("Config: team=%s test_style=%s created_lb=%s branches=%s", team, test_style, created_lb, branches_cfg)

    run_id = uuid.uuid4()

    cin7_auth = get_cin7_auth()
    sftp_cfg = get_sftp_cfg()

    conn = get_db_conn()
    psycopg2.extras.register_uuid()

    # If branches not provided in event, load from your branch map by instance
    with conn, conn.cursor() as cur:
        if not branches_cfg:
            cur.execute(
                "SELECT instance, array_agg(branch_id ORDER BY branch_id) "
                "FROM cin7_branch_map WHERE team_name=%s AND is_active GROUP BY instance",
                (team,)
            )
            for inst, arr in cur.fetchall():
                branches_cfg[inst] = arr or []
    if not branches_cfg:
        raise RuntimeError("No branches configured; populate cin7_branch_map or pass in event.branches")

    # Create sessions per instance
    sessions = {}
    for inst in branches_cfg.keys():
        user, pwd = cin7_auth[inst]
        sessions[inst] = Cin7Session(user, pwd, inst)

    # PRODUCTS (per instance) -> eligibility set
    elig_map: Dict[str, set] = {}
    for inst, sess in sessions.items():
        elig_map[inst] = pull_products(conn, team, run_id, sess, created_lb, test_style)

    # STOCK + ORDERS + ATP
    for inst, sess in sessions.items():
        bids = branches_cfg.get(inst, [])
        if not bids:
            continue
        pull_stock(conn, team, run_id, sess, bids, elig_map[inst])
        pull_purchase_orders(conn, team, run_id, sess, bids, elig_map[inst])
        pull_sales_orders(conn, team, run_id, sess, bids, elig_map[inst])
        compute_bucketed_atp(conn, team, run_id, inst)

    # CSV (by instance) using only DB names
    csv_rows: List[Tuple[str,str,str,str]] = []
    for inst in sessions.keys():
        csv_rows.extend(build_inventory_csv(conn, team, run_id, inst))

    # Exact CSV header + rows
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(["WareHouse", "StockItemKey", "AvailableDate", "Quantity"])
    w.writerows(csv_rows)
    payload = out.getvalue().encode("utf-8")
    out.close()

    # SFTP put to /data/source/inventory.csv
    remote_path = "/data/source/inventory.csv"
    sftp_put(get_sftp_cfg(), payload, remote_path)
    log.info("CSV rows written: %d (to %s)", len(csv_rows), remote_path)

    return {
        "run_id": str(run_id),
        "rows": len(csv_rows),
        "remote_path": remote_path,
    }

# ----------------------------
# Lambda entry
# ----------------------------
def lambda_handler(event, context):
    try:
        return run(event or {}, context)
    except Exception as e:
        log.exception("Run failed: %s", e)
        raise
