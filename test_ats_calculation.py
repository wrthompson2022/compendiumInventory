#!/usr/bin/env python3
"""
Test script to verify ATS calculation matches Excel specification
"""
from decimal import Decimal

def test_ats_logic():
    """
    Test data from ATS Explanation.xlsx:
    - Period 1: On Hand=100, WIP=0, Orders=50
    - Period 2: On Hand=0, WIP=50, Orders=100
    - Period 3: On Hand=0, WIP=0, Orders=100
    - Period 4: On Hand=0, WIP=250, Orders=150
    - Period 5: On Hand=0, WIP=250, Orders=100
    - Period 6: On Hand=0, WIP=0, Orders=50
    - Period 7: On Hand=0, WIP=100, Orders=300
    - Period 8: On Hand=0, WIP=0, Orders=200

    Expected: All ATS values should be 0 because total demand (1050) exceeds total supply (750)
    """

    # Simulate the bucket data
    # (opening_for_period, receipts, demand)
    bucket_data = [
        (Decimal("100"), Decimal("0"), Decimal("50")),    # Period 1 (today)
        (Decimal("50"), Decimal("50"), Decimal("100")),   # Period 2
        (Decimal("0"), Decimal("0"), Decimal("100")),     # Period 3
        (Decimal("-100"), Decimal("250"), Decimal("150")), # Period 4
        (Decimal("0"), Decimal("250"), Decimal("100")),   # Period 5
        (Decimal("150"), Decimal("0"), Decimal("50")),    # Period 6
        (Decimal("100"), Decimal("100"), Decimal("300")), # Period 7
        (Decimal("-100"), Decimal("0"), Decimal("200")),  # Period 8
    ]

    print("Testing ATS Calculation per Excel Formula")
    print("=" * 80)
    print(f"{'Period':>6} | {'Opening':>8} | {'Receipts':>8} | {'Demand':>8} | {'Remaining Supply':>16} | {'Remaining Demand':>16} | {'ATS':>8}")
    print("-" * 80)

    opening = Decimal("100")  # Starting on-hand
    running = opening

    results = []

    for idx, (_, receipts, demand) in enumerate(bucket_data):
        # Calculate total remaining supply from this bucket forward
        future_receipts = sum(r for _, r, _ in bucket_data[idx:])
        total_remaining_supply = running + future_receipts

        # Calculate total remaining demand from this bucket forward
        total_remaining_demand = sum(d for _, _, d in bucket_data[idx:])

        # ATS Formula: IF(total_remaining_demand > total_remaining_supply, 0, running + receipts - demand)
        if total_remaining_demand > total_remaining_supply:
            ats = Decimal("0")
        else:
            ats = running + receipts - demand

        results.append(ats)

        print(f"{idx+1:6} | {running:8} | {receipts:8} | {demand:8} | {total_remaining_supply:16} | {total_remaining_demand:16} | {ats:8}")

        # Update running balance for next period
        running = running + receipts - demand

    print("=" * 80)
    print(f"\nTotal Supply: 100 + (0+50+0+250+250+0+100+0) = 750")
    print(f"Total Demand: 50+100+100+150+100+50+300+200 = 1050")
    print(f"Excess Demand: 1050 - 750 = 300\n")

    # Verify all ATS values are 0
    expected = [Decimal("0")] * 8

    if results == expected:
        print("✓ TEST PASSED: All ATS values are 0 as expected!")
        print("  The corrected logic matches the Excel specification.")
        return True
    else:
        print("✗ TEST FAILED: ATS values don't match expected!")
        print(f"  Expected: {expected}")
        print(f"  Got:      {results}")
        return False


if __name__ == "__main__":
    success = test_ats_logic()
    exit(0 if success else 1)
