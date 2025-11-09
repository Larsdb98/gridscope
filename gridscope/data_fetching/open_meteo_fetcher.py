import requests
import pandas as pd
from datetime import datetime
from typing import List
from pathlib import Path


class OpenMeteoFetcher:
    def __init__(
        self,
        lattitude: float = 51.5,
        longitude: float = 0.1,
        start_date: str = "2024-10-01",
        end_date: str = "2024-10-10",
        hourly: List[str] = ["temperature_2m", "wind_speed_10m", "direct_radiation"],
        timezone: str = "Europe/London",
    ):
        self.fetched_weather_data: pd.DataFrame = None
        self.start_date = start_date
        self.end_date = end_date
        self.lattitude = lattitude
        self.longitude = longitude
        self.hourly = hourly
        self.timezone = timezone

    def fetch_weather_data(
        self, save_to_csv=False, dir_to_csv=Path("data/weather")
    ) -> pd.DataFrame:
        """
        Fetch hourly weather data (temperature, wind speed, solar radiation)
        for a given latitude/longitude and date range.
        """
        csv_filename = f"lat-{self.lattitude}_long-{self.longitude}_start-{self.start_date}_end-{self.end_date}.csv"
        csv_file = dir_to_csv / csv_filename

        base_url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": self.lattitude,
            "longitude": self.longitude,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": self.hourly,
            "timezone": self.timezone,
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # TODO: edit this below
        df = pd.DataFrame(
            {
                "datetime": data["hourly"]["time"],
                "temperature_2m": data["hourly"]["temperature_2m"],
                "wind_speed_10m": data["hourly"]["wind_speed_10m"],
                "direct_radiation": data["hourly"]["direct_radiation"],
            }
        )
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.to_csv(csv_file)

        return df


def main():
    fetcher = OpenMeteoFetcher()
    df_weather = fetcher.fetch_weather_data(save_to_csv=True)
    print(df_weather)


if __name__ == "__main__":
    main()
