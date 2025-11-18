import pandas as pd
from pathlib import Path
from entsog import EntsogPandasClient

import time


class UKGasImportsFetcher:
    """
    Fetch detailed UK gas import flows from ENTSOG Transparency Platform.
    Detection is based on entry point label matching, not country codes.
    """

    UK_IMPORT_KEYWORDS = [
        "bacton",
        "zeebrugge",
        "easington",
        "moffat",
        "isle of grain",
        "milford",
        "south hook",
        "dragon",
        "st. fergus",
        "teesside",
        "langeled",
        "bbl",
    ]

    def __init__(self, start_date: str, end_date: str, save_dir: str = "data/gas"):
        self.start_date = start_date
        self.end_date = end_date
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.client = EntsogPandasClient()

    def _build_uk_point_direction_keys(self):
        print("Querying ENTSOG operator point directions...")

        points = self.client.query_operator_point_directions()
        labels = points["point_label"].str.lower()

        gb_points = points[
            (points["direction_key"] == "entry")
            & (labels.str.contains("|".join(self.UK_IMPORT_KEYWORDS)))
        ]

        if gb_points.empty:
            raise RuntimeError("No UK import entry points found!")

        print(f"Found {len(gb_points)} UK import entry points:")
        for label in gb_points["point_label"].unique():
            print(f"  • {label}")

        # Build point keys for query
        keys = [
            f"{row['operator_key']}{row['point_key']}{row['direction_key']}"
            for _, row in gb_points.iterrows()
        ]
        return gb_points, keys

    # ATTEMPTING TO MERGE SITE MAPPINGS TO GET NICE LABELS INSTEAD OF DEALING WITH CRYPTING EIC-LIKE IDs

    # def fetch(self) -> pd.DataFrame:
    #     print(f"\nFetching ENTSOG UK imports {self.start_date} → {self.end_date} ...")

    #     gb_points, keys = self._build_uk_point_direction_keys()

    #     # --------------------------------------------------------------
    #     # Build monthly time chunks to avoid Gateway Timeout (504)
    #     # --------------------------------------------------------------
    #     dates = pd.date_range(self.start_date, self.end_date, freq="MS")
    #     if dates[-1] < pd.Timestamp(self.end_date):
    #         dates = dates.append(pd.DatetimeIndex([self.end_date]))

    #     frames = []

    #     for i in range(len(dates) - 1):
    #         start = dates[i].tz_localize("Europe/Brussels")
    #         end = dates[i + 1].tz_localize("Europe/Brussels")

    #         print(f" - Querying {start.date()} → {end.date()} ...")

    #         try:
    #             df_chunk = self.client.query_operational_point_data(
    #                 start=start,
    #                 end=end,
    #                 indicators=["physical_flow"],
    #                 point_directions=keys,
    #                 verbose=False,
    #             )
    #         except Exception as e:
    #             print(f"    !! Failed for {start.date()} → {end.date()} : {e}")
    #             continue

    #         if df_chunk.empty:
    #             print(f"    (no data returned)")
    #             continue

    #         frames.append(df_chunk)

    #     if not frames:
    #         raise RuntimeError("No ENTSOG gas flow data returned for any period.")

    #     # Combine all chunks
    #     df = pd.concat(frames, ignore_index=True)

    #     print(f"Retrieved {len(df)} ENTSOG flow rows.")
    #     print(f"DF Columns: {list(df.columns)}")

    #     # --------------------------------------------------------------
    #     # Step 1 — Identify timestamp column and normalize it
    #     # --------------------------------------------------------------
    #     ts_candidates = ["period_from", "periodFrom", "gasDayStart", "timestamp"]
    #     ts_col = next((c for c in ts_candidates if c in df.columns), None)

    #     if ts_col is None:
    #         raise RuntimeError(
    #             f"No usable timestamp column found. Available: {list(df.columns)}"
    #         )

    #     df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    #     df = df.dropna(subset=[ts_col])

    #     # Convert to naive UTC datetime
    #     df["timestamp"] = df[ts_col].dt.tz_convert(None)

    #     # --------------------------------------------------------------
    #     # Step 2 — Ensure numeric flow values
    #     # --------------------------------------------------------------
    #     df["value"] = pd.to_numeric(df["value"], errors="coerce")
    #     df = df.dropna(subset=["value"])

    #     # --------------------------------------------------------------
    #     # Step 3 — Convert to MWh/h
    #     # --------------------------------------------------------------
    #     unit = str(df["unit"].iloc[0]).lower()

    #     if "kwh" in unit:
    #         df["MWh_hour"] = df["value"] / 1000.0
    #     elif "mwh" in unit:
    #         df["MWh_hour"] = df["value"]
    #     else:
    #         print(f"⚠ Unknown ENTSOG unit '{unit}', treating as MWh.")
    #         df["MWh_hour"] = df["value"]

    #     # --------------------------------------------------------------
    #     # Step 4 — Normalize keys and join point labels
    #     # --------------------------------------------------------------
    #     for col in ["operator_key", "point_key", "direction_key"]:
    #         if col in df.columns:
    #             df[col] = df[col].astype(str).str.strip()

    #     for col in ["operator_key", "point_key", "direction_key"]:
    #         if col in gb_points.columns:
    #             gb_points[col] = gb_points[col].astype(str).str.strip()

    #     # First try full key match
    #     merged = df.merge(
    #         gb_points[["operator_key", "point_key", "direction_key", "point_label"]],
    #         how="left",
    #         on=["operator_key", "point_key", "direction_key"],
    #         validate="many_to_one",  # ensures consistent mapping
    #     )

    #     # If still missing labels, fallback to point_key only
    #     if merged["point_label"].isna().any():
    #         fallback = gb_points.drop_duplicates("point_key")[
    #             ["point_key", "point_label"]
    #         ]
    #         merged = merged.merge(
    #             fallback, on="point_key", how="left", suffixes=("", "_fallback")
    #         )
    #         merged["point_label"] = merged["point_label"].fillna(
    #             merged["point_label_fallback"]
    #         )
    #         merged = merged.drop(columns=["point_label_fallback"], errors="ignore")

    #     # Replace any still-missing labels
    #     merged["point_label"] = merged["point_label"].fillna("Unknown_Point")

    #     df = merged  # final merged frame

    #     # --------------------------------------------------------------
    #     # Step 5 — Pivot to hourly import matrix
    #     # --------------------------------------------------------------
    #     pivot = df.pivot_table(
    #         index="timestamp",
    #         columns="point_label",
    #         values="MWh_hour",
    #         aggfunc="sum",
    #     ).sort_index()

    #     pivot = pivot.asfreq("H").fillna(0)

    #     print(f"Final import matrix shape: {pivot.shape}")

    #     return pivot
    def fetch(self) -> pd.DataFrame:
        print(f"\nFetching ENTSOG UK imports {self.start_date} → {self.end_date} ...")

        gb_points, keys = self._build_uk_point_direction_keys()

        # --------------------------------------------------------------
        # Build monthly time chunks to avoid Gateway Timeout (504)
        # --------------------------------------------------------------
        dates = pd.date_range(self.start_date, self.end_date, freq="MS")
        if dates[-1] < pd.Timestamp(self.end_date):
            dates = dates.append(pd.DatetimeIndex([self.end_date]))

        frames = []

        for i in range(len(dates) - 1):
            start = dates[i].tz_localize("Europe/Brussels")
            end = dates[i + 1].tz_localize("Europe/Brussels")

            print(f" - Querying {start.date()} → {end.date()} ...")

            try:
                df_chunk = self.client.query_operational_point_data(
                    start=start,
                    end=end,
                    indicators=["physical_flow"],
                    point_directions=keys,
                    verbose=False,
                )
            except Exception as e:
                print(f"    !! Failed for {start.date()} → {end.date()} : {e}")
                continue

            if df_chunk.empty:
                print("    (no data returned)")
                continue

            frames.append(df_chunk)

        if not frames:
            raise RuntimeError("No ENTSOG gas flow data returned for any period.")

        df = pd.concat(frames, ignore_index=True)

        print(f"Retrieved {len(df)} ENTSOG flow rows.")
        print(f"DF Columns: {list(df.columns)}")

        # --------------------------------------------------------------
        # Step 1 — Convert timestamp field
        # --------------------------------------------------------------
        ts_candidates = ["period_from", "periodFrom", "gasDayStart", "timestamp"]
        ts_col = next((c for c in ts_candidates if c in df.columns), None)

        if ts_col is None:
            raise RuntimeError(
                f"No usable timestamp column found. Available columns: {list(df.columns)}"
            )

        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
        df = df.dropna(subset=[ts_col])
        df["timestamp"] = df[ts_col].dt.tz_convert(None)

        # --------------------------------------------------------------
        # Step 2 — convert flow to MWh/h and aggregate total for UK
        # --------------------------------------------------------------
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

        unit = str(df["unit"].iloc[0]).lower()

        if "kwh" in unit:
            df["MWh_hour"] = df["value"] / 1000.0
        else:
            df["MWh_hour"] = df["value"]

        # --------------------------------------------------------------
        # Step 3 — Aggregate to TOTAL UK imports per hour
        # --------------------------------------------------------------
        df_hourly = (
            df.groupby(df["timestamp"])["MWh_hour"]
            .sum()
            .rename("UK_imports_MWh_hour")
            .to_frame()
            .sort_index()
        )

        # --------------------------------------------------------------
        # Optionally resample to 30-min here if desired
        # df_halfhour = df_hourly.resample("30T").interpolate()
        # --------------------------------------------------------------

        print(f"Final hourly records: {len(df_hourly)}")
        return df_hourly

    def save(self, df: pd.DataFrame):
        file = self.save_dir / f"uk_gas_imports_{self.start_date}_{self.end_date}.csv"
        df.to_csv(file)
        print(f"Saved UK gas imports → {file}")

    def run(self):
        df = self.fetch()
        self.save(df)
        return df


def main():
    fetcher = UKGasImportsFetcher(
        start_date="2020-01-01",
        end_date="2020-12-31",
    )
    result = fetcher.run()
    time.sleep(61)

    fetcher = UKGasImportsFetcher(
        start_date="2021-01-01",
        end_date="2021-12-31",
    )
    result = fetcher.run()
    time.sleep(61)

    fetcher = UKGasImportsFetcher(
        start_date="2022-01-01",
        end_date="2022-12-31",
    )
    result = fetcher.run()
    time.sleep(61)

    fetcher = UKGasImportsFetcher(
        start_date="2023-01-01",
        end_date="2023-12-31",
    )
    result = fetcher.run()
    time.sleep(61)

    fetcher = UKGasImportsFetcher(
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    result = fetcher.run()


if __name__ == "__main__":
    main()
