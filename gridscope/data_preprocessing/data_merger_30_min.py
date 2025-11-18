import pandas as pd
import os
from pathlib import Path
import numpy as np

from typing import List


class DataMerger30Min:
    def __init__(
        self,
        uk_weather_csv_paths: List[str],
        neso_demand_csv_paths: List[str],
        uk_gas_imports_csv_paths: List[str],
        uk_gas_prices_csv_path: str,
    ):
        self.uk_weather_csv_paths = uk_weather_csv_paths
        self.neso_demand_csv_paths = neso_demand_csv_paths
        self.uk_gas_imports_csv_paths = uk_gas_imports_csv_paths
        self.gas_prices_csv_path = uk_gas_prices_csv_path


def main():
    uk_weather_csv_paths = [
        "data/weather/uk_raw_weather_sites_start-2020-01-01_end-2020-12-31.csv",
        "data/weather/uk_raw_weather_sites_start-2021-01-01_end-2021-12-31.csv",
        "data/weather/uk_raw_weather_sites_start-2022-01-01_end-2022-12-31.csv",
        "data/weather/uk_raw_weather_sites_start-2023-01-01_end-2023-12-31.csv",
        "data/weather/uk_raw_weather_sites_start-2023-01-01_end-2023-12-31.csv",
    ]

    neso_demand_csv_paths = [
        "data/neso/historical_demand/demanddata_2020.csv",
        "data/neso/historical_demand/demanddata_2021.csv",
        "data/neso/historical_demand/demanddata_2022.csv",
        "data/neso/historical_demand/demanddata_2023.csv",
        "data/neso/historical_demand/demanddata_2024.csv",
    ]
    uk_gas_imports_csv_paths = [
        "data/gas/uk_gas_imports_2020-01-01_2020-12-31.csv",
        "data/gas/uk_gas_imports_2021-01-01_2021-12-31.csv",
        "data/gas/uk_gas_imports_2022-01-01_2022-12-31.csv",
        "data/gas/uk_gas_imports_2023-01-01_2023-12-31.csv",
        "data/gas/uk_gas_imports_2024-01-01_2024-12-31.csv",
    ]
    uk_gas_prices_csv_path = "data/gas/processed/sap_gas_30min.csv"

    merger = DataMerger30Min(
        uk_weather_csv_paths=uk_weather_csv_paths,
        neso_demand_csv_paths=neso_demand_csv_paths,
        uk_gas_imports_csv_paths=uk_gas_imports_csv_paths,
        uk_gas_prices_csv_path=uk_gas_prices_csv_path,
    )


if __name__ == "__main__":
    main()
