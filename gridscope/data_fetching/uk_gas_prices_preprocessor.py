import pandas as pd
from pathlib import Path
from datetime import datetime


class GasPricePreprocessor:
    """
    Loads a local CSV of UK SAP gas prices (p/kWh), converts date column,
    produces a cleaned daily dataset, and optionally interpolates to 30-minute resolution.
    """

    def __init__(
        self,
        csv_path: str | Path,
        date_col: str = "Date",
        value_col: str = "SAP actual day",
        convert_to_gbp_mwh: bool = True,
    ):
        self.csv_path = Path(csv_path)
        self.date_col = date_col
        self.value_col = value_col
        self.convert_to_gbp_mwh = convert_to_gbp_mwh

        self.df = None  # daily dataset

    # ------------------------------------------------------------------
    def load(self) -> pd.DataFrame:
        """
        Loads and cleans the raw daily SAP dataset.
        """
        if not self.csv_path.exists():
            raise FileNotFoundError(f"SAP CSV not found: {self.csv_path}")

        df = pd.read_csv(self.csv_path)

        # Convert DD.MM.YY → datetime
        df[self.date_col] = pd.to_datetime(df[self.date_col], dayfirst=True)

        # Keep only the date + value
        df = df[[self.date_col, self.value_col]].rename(
            columns={
                self.date_col: "date",
                self.value_col: "SAP_p_per_kWh",
            }
        )

        df = df.set_index("date").sort_index()

        # Convert p/kWh → GBP/MWh
        if self.convert_to_gbp_mwh:
            df["SAP_GBP_per_MWh"] = df["SAP_p_per_kWh"] * 10
        else:
            df["SAP_GBP_per_MWh"] = df["SAP_p_per_kWh"]

        self.df = df
        return df

    # ------------------------------------------------------------------
    def interpolate_half_hourly(self, method: str = "linear") -> pd.DataFrame:
        """
        Converts daily dataset → 30-minute intervals using interpolation.
        """
        if self.df is None:
            raise ValueError("Load data first using .load()")

        df_resampled = self.df.resample("30min").interpolate(method)
        return df_resampled

    # ------------------------------------------------------------------
    def save(self, df: pd.DataFrame, output_path: str | Path) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path)
        print(f"Saved gas price data → {output_path}")


def main():
    processor = GasPricePreprocessor(
        csv_path="data/gas/systemaveragepriceofgasdataset131125.csv",
        convert_to_gbp_mwh=True,
    )

    # Load daily dataset
    df_daily = processor.load()

    # Create 30-minute dataset
    df_30min = processor.interpolate_half_hourly(method="cubic")

    # Save both
    processor.save(df_daily, "data/gas/processed/sap_gas_daily.csv")
    processor.save(df_30min, "data/gas/processed/sap_gas_30min.csv")

    print(df_daily.head())
    print(df_30min.head())


if __name__ == "__main__":
    main()
