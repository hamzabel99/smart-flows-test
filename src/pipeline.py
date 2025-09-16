import polars as pl


def load_data(file_path: str) -> pl.DataFrame:
    return pl.read_csv(file_path, try_parse_dates=True)


def calculate_total_per_sensor(data: pl.DataFrame) -> pl.DataFrame:
    return data.group_by("sensor_id").agg(
        pl.col("passengers").sum().alias("total_passengers")
    )


def calculate_average_per_sensor(data: pl.DataFrame) -> pl.DataFrame:
    return data.group_by("sensor_id").agg(
        pl.col("passengers").mean().alias("average_passengers")
    )


def save_results(total_data: pl.DataFrame, average_data: pl.DataFrame,
                 total_file="total_passagers.csv", avg_file="moyenne_passagers.csv") -> None:
    total_data.write_csv(total_file)
    average_data.write_csv(avg_file)


def main(csv_path: str) -> None:
    data = load_data(csv_path)
    
    total_par_capteur = calculate_total_per_sensor(data)
    moyenne_par_capteur = calculate_average_per_sensor(data)
    
    save_results(total_par_capteur, moyenne_par_capteur)
    
    print("Total passagers par capteur:")
    print(total_par_capteur)
    print("\nMoyenne passagers par capteur:")
    print(moyenne_par_capteur)


if __name__ == "__main__":
    csv_path = ""  #Path to declare
    main(csv_path)
