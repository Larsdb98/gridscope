# src/data/weather_multi.py
import requests
import pandas as pd
from pathlib import Path
from typing import List
import time


# OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"


class OpenMeteoFetcherUk:
    """
    Fetches raw weather data (temperature, wind speed, solar radiation)
    for multiple UK locations. Each site's variables are stored in separate columns.
    """

    OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, start_date: str = "2024-10-01", end_date: str = "2024-10-15"):
        self.start_date = start_date
        self.end_date = end_date

        # ---- Demand-related temperature sites (major UK cities) ----
        self.DEMAND_TEMP_SITES = [
            (51.5072, -0.1276),  # London
            (52.4862, -1.8904),  # Birmingham
            (53.4808, -2.2426),  # Manchester
            (53.8008, -1.5491),  # Leeds
            (55.8642, -4.2518),  # Glasgow
            (53.4084, -2.9916),  # Liverpool
            (51.4545, -2.5879),  # Bristol
            (55.9533, -3.1883),  # Edinburgh
            (54.9783, -1.6178),  # Newcastle
            (53.3811, -1.4701),  # Sheffield
        ]

        # ---- Wind farm regions (onshore & offshore) ----
        self.WIND_SITES = [
            (54.6, 1.7),  # Dogger Bank
            (53.5, 1.5),  # Hornsea / East Anglia
            (58.1, -2.5),  # Moray Firth
            (53.6, -3.6),  # Liverpool Bay
            (57.3, -2.4),  # Aberdeenshire
            (57.5, -4.2),  # Highlands
            (53.6, -0.7),  # Yorkshire/Humber
            (52.3, -3.9),  # Wales
        ]

        # ---- Solar-heavy regions (southern UK) ----
        self.SOLAR_SITES = [
            (51.2, 0.7),  # Kent
            (51.7, 0.3),  # Essex
            (51.0, -1.3),  # Hampshire
            (52.4, 1.0),  # East Anglia
            (52.5, -1.9),  # Midlands
            (50.8, -3.5),  # South West
            (51.6, -3.0),  # South Wales
        ]

        self.weather_df: pd.DataFrame = None

    # ----------------------------------------------------------------
    # Internal utility: fetch one site
    # ----------------------------------------------------------------
    def fetch_openmeteo_hourly(
        self,
        lat: float,
        lon: float,
        hourly: List[str],
        timezone: str = "Europe/London",
    ) -> pd.DataFrame:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": ",".join(hourly),
            "timezone": timezone,
        }
        r = requests.get(self.OPEN_METEO_ARCHIVE, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        df = pd.DataFrame({"datetime": data["hourly"]["time"]})
        for v in hourly:
            df[v] = data["hourly"][v]
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df.set_index("datetime").sort_index()

    def build_weather_dataset(self, interpolate_to_30min: bool = True) -> pd.DataFrame:
        all_dfs = []

        # ===============================================================
        # Demand temperature sites (major cities)
        # ESSENTIAL VARIABLES:
        # - temperature_2m (°C)
        # - relative_humidity_2m (%)
        # - precipitation (mm)
        # ===============================================================
        for i, (lat, lon) in enumerate(self.DEMAND_TEMP_SITES, start=1):
            df_temp = self.fetch_openmeteo_hourly(
                lat,
                lon,
                [
                    "temperature_2m",  # ESSENTIAL: demand driver
                    "relative_humidity_2m",  # ESSENTIAL: complements temperature
                    "precipitation",  # ESSENTIAL: cold/rainy regime proxy
                    "dew_point_2m",
                    "pressure_msl",
                    "cloud_cover",
                ],
            )
            df_temp = df_temp.rename(
                columns={
                    "temperature_2m": f"temp_site{i}",
                    "relative_humidity_2m": f"humidity_site{i}",
                    "precipitation": f"precip_site{i}",
                    "dew_point_2m": f"dewpoint_site{i}",
                    "pressure_msl": f"pressure_site{i}",
                    "cloud_cover": f"cloud_site{i}",
                }
            )
            all_dfs.append(df_temp)

        # ===============================================================
        # Wind sites (offshore + onshore)
        # ESSENTIAL VARIABLES:
        # - wind_speed_100m (m/s)
        # - wind_direction_100m (°)
        # - pressure_msl (hPa)
        # ===============================================================
        for i, (lat, lon) in enumerate(self.WIND_SITES, start=1):
            df_wind = self.fetch_openmeteo_hourly(
                lat,
                lon,
                [
                    "wind_speed_100m",  # ESSENTIAL: generation proxy
                    "wind_direction_100m",  # ESSENTIAL: regional correlation
                    "pressure_msl",  # ESSENTIAL: regime classifier
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                ],
            )
            df_wind = df_wind.rename(
                columns={
                    "wind_speed_100m": f"wind_speed_site{i}",
                    "wind_direction_100m": f"wind_dir_site{i}",
                    "pressure_msl": f"pressure_site{i}",
                    "temperature_2m": f"temp_site_wind{i}",
                    "relative_humidity_2m": f"humidity_site_wind{i}",
                    "precipitation": f"precip_site_wind{i}",
                }
            )
            all_dfs.append(df_wind)

        # ===============================================================
        # Solar sites (southern UK)
        # ESSENTIAL VARIABLES:
        # - shortwave_radiation (W/m²)
        # - cloud_cover (%)
        # ===============================================================
        for i, (lat, lon) in enumerate(self.SOLAR_SITES, start=1):
            df_solar = self.fetch_openmeteo_hourly(
                lat,
                lon,
                [
                    "shortwave_radiation",  # ESSENTIAL: solar irradiance proxy
                    "cloud_cover",  # ESSENTIAL: PV generation dampening
                    "pressure_msl",
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                ],
            )
            df_solar = df_solar.rename(
                columns={
                    "shortwave_radiation": f"solar_rad_site{i}",
                    "cloud_cover": f"cloud_site_solar{i}",
                    "pressure_msl": f"pressure_site_solar{i}",
                    "temperature_2m": f"temp_site_solar{i}",
                    "relative_humidity_2m": f"humidity_site_solar{i}",
                    "precipitation": f"precip_site_solar{i}",
                }
            )
            all_dfs.append(df_solar)

        df_all = pd.concat(all_dfs, axis=1)

        if interpolate_to_30min:
            # Optional: convert to settlement half-hours
            df_all = df_all.resample("30T").interpolate("time")

        self.weather_df = df_all
        return df_all

    # ----------------------------------------------------------------
    # Save to CSV
    # ----------------------------------------------------------------
    def save_to_csv(self, dir_to_csv=Path("data/weather")) -> None:
        if self.weather_df is None:
            raise ValueError("No weather data has been fetched yet!")

        dir_to_csv.mkdir(parents=True, exist_ok=True)
        csv_filename = (
            f"uk_raw_weather_sites_start-{self.start_date}_end-{self.end_date}.csv"
        )
        csv_file = dir_to_csv / csv_filename
        self.weather_df.to_csv(csv_file)
        print(f"Weather data saved to {csv_file}")


def fetch_2019_2024(pause_time: int = 60) -> None:
    fetcher_1 = OpenMeteoFetcherUk(start_date="2019-01-01", end_date="2019-12-31")
    fetcher_2 = OpenMeteoFetcherUk(start_date="2020-01-01", end_date="2020-12-31")
    fetcher_3 = OpenMeteoFetcherUk(start_date="2021-01-01", end_date="2021-12-31")
    fetcher_4 = OpenMeteoFetcherUk(start_date="2022-01-01", end_date="2022-12-31")
    fetcher_5 = OpenMeteoFetcherUk(start_date="2023-01-01", end_date="2023-12-31")
    fetcher_6 = OpenMeteoFetcherUk(start_date="2024-01-01", end_date="2024-12-31")

    # fetch & save
    _ = fetcher_1.build_weather_dataset()
    time.sleep(pause_time)
    _ = fetcher_2.build_weather_dataset()
    time.sleep(pause_time)
    _ = fetcher_3.build_weather_dataset()
    time.sleep(pause_time)
    _ = fetcher_4.build_weather_dataset()
    time.sleep(pause_time)
    _ = fetcher_5.build_weather_dataset()
    time.sleep(pause_time)
    _ = fetcher_6.build_weather_dataset()

    fetcher_1.save_to_csv()
    fetcher_2.save_to_csv()
    fetcher_3.save_to_csv()
    fetcher_4.save_to_csv()
    fetcher_5.save_to_csv()
    fetcher_6.save_to_csv()


def main() -> int:
    if 0:
        fetcher = OpenMeteoFetcherUk()
        # features = fetcher.build_weather_features()
        features = fetcher.build_weather_dataset()
        fetcher.save_to_csv()
        print(features.head())
    else:
        fetch_2019_2024(pause_time=61)

    return 0


if __name__ == "__main__":
    main()
