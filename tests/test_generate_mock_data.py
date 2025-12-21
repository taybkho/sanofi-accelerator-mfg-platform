"""
Simple tests for the mock data generator.

These tests run after calling generate_mock_data.py and make sure:
- CSV files exist
- They have at least a minimum number of rows
- Required columns are present
"""


from pathlib import Path

import pandas as pd


BASE_DIR = Path("data") / "raw" / "source_system"


# def _load_csv(filename: str) -> pd.DataFrame:
#     path = BASE_DIR / filename
#     assert path.exists(), f"Expected file does not exist: {path}"
#     df = pd.read_csv(path)
#     return df

def _load_csv(filename: str) -> pd.DataFrame:
    path = BASE_DIR / filename

    if not path.exists():
        raise FileNotFoundError(f"Expected file does not exist: {path}")

    df = pd.read_csv(path)
    return df



def test_materials_csv_shape_and_columns():
    df = _load_csv("materials.csv")
    # At least some rows
    assert len(df) >= 100
    # Required columns
    expected_cols = {
        "material_id",
        "material_name",
        "material_type",
        "unit_of_measure",
        "status",
        "created_at",
    }
    assert expected_cols.issubset(set(df.columns))


def test_batches_csv_shape_and_columns():
    df = _load_csv("batches.csv")
    assert len(df) >= 1000
    expected_cols = {
        "batch_id",
        "material_id",
        "manufacturing_site",
        "manufacture_date",
        "expiry_date",
        "quantity",
        "batch_status",
    }
    assert expected_cols.issubset(set(df.columns))


def test_production_orders_csv_shape_and_columns():
    df = _load_csv("production_orders.csv")
    assert len(df) >= 1000
    expected_cols = {
        "order_id",
        "batch_id",
        "order_type",
        "planned_start_date",
        "planned_end_date",
        "actual_start_date",
        "actual_end_date",
        "order_status",
        "line_id",
    }
    assert expected_cols.issubset(set(df.columns))
