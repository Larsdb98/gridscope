import pandas as pd
import numpy as np
from typing import List


class DataMergerDaily:
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
        self.uk_gas_prices_csv_path = uk_gas_prices_csv_path

    # ---------------------------------------------------------
    # 1) Load and concatenate multiple CSV files
    # ---------------------------------------------------------
    def _load_concat(self, paths: List[str], parse_dates=None) -> pd.DataFrame:
        frames = []
        for p in paths:
            df = pd.read_csv(p, parse_dates=parse_dates, low_memory=False)
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    # ---------------------------------------------------------
    # 2) Weather → aggregate daily means
    # ---------------------------------------------------------
    def load_weather_daily(self) -> pd.DataFrame:
        df = self._load_concat(self.uk_weather_csv_paths, parse_dates=["datetime"])
        df = df.rename(columns={"datetime": "timestamp"})
        df["date"] = df["timestamp"].dt.date
        df = df.drop(columns=["timestamp"])

        # Daily average for all columns except the date
        daily = df.groupby("date").mean(numeric_only=True)
        daily.index = pd.to_datetime(daily.index)
        return daily.reset_index().rename(columns={"date": "timestamp"})

    # ---------------------------------------------------------
    # 3) NESO → convert SP to timestamp, then aggregate daily
    # ---------------------------------------------------------
    def load_neso_daily(self) -> pd.DataFrame:
        df = self._load_concat(self.neso_demand_csv_paths)

        df["SETTLEMENT_DATE"] = pd.to_datetime(
            df["SETTLEMENT_DATE"], errors="coerce", format="%d-%b-%Y"
        )
        df["timestamp"] = df["SETTLEMENT_DATE"] + pd.to_timedelta(
            (df["SETTLEMENT_PERIOD"] - 1) * 30, unit="m"
        )
        df["date"] = df["timestamp"].dt.date

        # Fill missing columns as before
        required = [
            "ND",
            "TSD",
            "ENGLAND_WALES_DEMAND",
            "EMBEDDED_WIND_GENERATION",
            "EMBEDDED_WIND_CAPACITY",
            "EMBEDDED_SOLAR_GENERATION",
            "EMBEDDED_SOLAR_CAPACITY",
            "NON_BM_STOR",
            "PUMP_STORAGE_PUMPING",
            "SCOTTISH_TRANSFER",
            "IFA_FLOW",
            "IFA2_FLOW",
            "BRITNED_FLOW",
            "MOYLE_FLOW",
            "EAST_WEST_FLOW",
            "NEMO_FLOW",
            "NSL_FLOW",
            "ELECLINK_FLOW",
            "VIKING_FLOW",
            "GREENLINK_FLOW",
        ]
        for col in required:
            if col not in df.columns:
                df[col] = 0.0

        # Daily aggregation rules
        agg_rules = {
            # Demand — take mean over the day
            "ND": "mean",
            "TSD": "mean",
            "ENGLAND_WALES_DEMAND": "mean",
            # Embedded gen/capacity — mean
            "EMBEDDED_WIND_GENERATION": "mean",
            "EMBEDDED_WIND_CAPACITY": "mean",
            "EMBEDDED_SOLAR_GENERATION": "mean",
            "EMBEDDED_SOLAR_CAPACITY": "mean",
            # Storage — sum (energy)
            "NON_BM_STOR": "sum",
            "PUMP_STORAGE_PUMPING": "sum",
            # Flows — sum across all periods
            "SCOTTISH_TRANSFER": "sum",
            "IFA_FLOW": "sum",
            "IFA2_FLOW": "sum",
            "BRITNED_FLOW": "sum",
            "MOYLE_FLOW": "sum",
            "EAST_WEST_FLOW": "sum",
            "NEMO_FLOW": "sum",
            "NSL_FLOW": "sum",
            "ELECLINK_FLOW": "sum",
            "VIKING_FLOW": "sum",
            "GREENLINK_FLOW": "sum",
        }

        daily = df.groupby("date").agg(agg_rules)
        daily.index = pd.to_datetime(daily.index)
        return daily.reset_index().rename(columns={"date": "timestamp"})

    # ---------------------------------------------------------
    # 4) Gas imports are already daily
    # ---------------------------------------------------------
    def load_gas_imports_daily(self) -> pd.DataFrame:
        df = self._load_concat(self.uk_gas_imports_csv_paths, parse_dates=["timestamp"])
        df = df.rename(columns={"UK_imports_MWh_hour": "gas_imports_MWh_daily"})
        df["timestamp"] = df["timestamp"].dt.date
        df = df.groupby("timestamp").first().reset_index()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    # ---------------------------------------------------------
    # 5) Gas prices are daily
    # ---------------------------------------------------------
    def load_gas_prices_daily(self) -> pd.DataFrame:
        df = pd.read_csv(self.uk_gas_prices_csv_path, parse_dates=["date"])
        df = df.rename(columns={"date": "timestamp"})
        df["timestamp"] = df["timestamp"].dt.date
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df[["timestamp", "SAP_p_per_kWh", "SAP_GBP_per_MWh"]]

    # ---------------------------------------------------------
    # 6) Merge all into a daily dataset
    # ---------------------------------------------------------
    def merge(self) -> pd.DataFrame:
        print("Loading daily datasets...")

        weather = self.load_weather_daily()
        neso = self.load_neso_daily()
        gas_imports = self.load_gas_imports_daily()
        gas_prices = self.load_gas_prices_daily()

        # Build full daily index
        start = pd.to_datetime("2020-01-01")
        end = pd.to_datetime("2024-12-31")
        idx = pd.date_range(start, end, freq="D")

        base = pd.DataFrame({"timestamp": idx})

        print("Merging weather daily...")
        base = base.merge(weather, on="timestamp", how="left")

        print("Merging NESO daily...")
        base = base.merge(neso, on="timestamp", how="left")

        print("Merging gas imports daily...")
        base = base.merge(gas_imports, on="timestamp", how="left")

        print("Merging gas prices daily...")
        base = base.merge(gas_prices, on="timestamp", how="left")

        # Forward-fill numeric columns
        numeric_cols = base.select_dtypes(include=[np.number]).columns
        base[numeric_cols] = base[numeric_cols].fillna(method="ffill")

        print("Final shape:", base.shape)
        return base

    # ---------------------------------------------------------
    # 7) Save
    # ---------------------------------------------------------
    def save(self, output_path: str):
        df = self.merge()
        df.to_csv(output_path, index=False)
        print(f"Saved daily dataset → {output_path}")


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
    uk_gas_prices_csv_path = "data/gas/processed/sap_gas_daily.csv"

    merger = DataMergerDaily(
        uk_weather_csv_paths,
        neso_demand_csv_paths,
        uk_gas_imports_csv_paths,
        uk_gas_prices_csv_path,
    )

    merger.save("data/full_datasets/full_uk_energy_daily_2020_2024.csv")


if __name__ == "__main__":
    main()
