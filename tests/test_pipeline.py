import polars as pl
import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline import load_data, calculate_total_per_sensor, calculate_average_per_sensor, save_results


@pytest.fixture
def sample_data():
    return pl.DataFrame({
        "timestamp": ["2023-09-01T10:00:00Z"] * 5,
        "sensor_id": [1, 1, 2, 2, 2],
        "passengers": [10, 20, 5, 15, 10]
    })


def test_calculate_total_per_sensor(sample_data):
    result = calculate_total_per_sensor(sample_data)
    expected = pl.DataFrame({
        "sensor_id": [1, 2],
        "total_passengers": [30, 30]
    })
    
    assert result.sort("sensor_id").to_dicts() == expected.sort("sensor_id").to_dicts()


def test_calculate_average_per_sensor(sample_data):
    result = calculate_average_per_sensor(sample_data)
    expected = pl.DataFrame({
        "sensor_id": [1, 2],
        "average_passengers": [15.0, 10.0]
    })
    assert result.sort("sensor_id").to_dicts() == expected.sort("sensor_id").to_dicts()


def test_save_results(sample_data, tmp_path):
    total = calculate_total_per_sensor(sample_data)
    avg = calculate_average_per_sensor(sample_data)
    
    total_file = tmp_path / "total.csv"
    avg_file = tmp_path / "avg.csv"
    
    save_results(total, avg, total_file, avg_file)
    
    total_loaded = pl.read_csv(total_file)
    avg_loaded = pl.read_csv(avg_file)
    
    assert total.sort("sensor_id").to_dicts() == total_loaded.sort("sensor_id").to_dicts()
    assert avg.sort("sensor_id").to_dicts() == avg_loaded.sort("sensor_id").to_dicts()


def test_load_data(tmp_path):

    file_path = tmp_path / "data.csv"
    df = pl.DataFrame({"sensor_id": [1,2], "passengers": [10, 20]})
    df.write_csv(file_path)
    
    loaded = load_data(file_path)
    assert df.to_dicts() == loaded.to_dicts()
