import requests
import pandas as pd
from typing import Optional


# This script is far from ready since there are a lot of ressources already available to download directly as a csv for training
# It can be ignored for now. Check out the notes.txt for links to data that was pulled instead.
# This script will be repurposed for daily fetching instead and using SQL queries.


class NesoDemandFetcher:
    def __init__(self, resource_id: str):
        self.BASE_URL = "https://api.neso.energy/api/3/action/"
        self.resource_id = resource_id

    def get_resource_metadata(self) -> dict:
        """
        Downloads resource metadata (e.g., last modified date) for a resource.
        """
        url = self.BASE_URL + "resource_show"
        params = {"id": self.resource_id}
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()["result"]

    def fetch_demand_data(
        self, limit: int = 10000, filters: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Fetches demand data from NESO for the given resource_id.
        You can use filters to narrow it (e.g., date). This option may be removed in the future. But chatgpt is pretty nice to integrate it here.
        It may not be the right place for it.
        """
        url = self.BASE_URL + "datastore_search"
        params = {"resource_id": self.resource_id, "limit": limit}
        if filters:
            params["filters"] = filters

        r = requests.get(url, params=params)
        r.raise_for_status()
        records = r.json()["result"]["records"]
        df = pd.DataFrame(records)

        # Try to find a timestamp column
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
        else:
            # Example fallback:
            if "SETTLEMENT_DATE" in df.columns and "SETTLEMENT_PERIOD" in df.columns:
                df["datetime"] = pd.to_datetime(
                    df["SETTLEMENT_DATE"]
                    + " "
                    + df["SETTLEMENT_PERIOD"].astype(str)
                    + ":00",
                    format="%Y-%m-%d %H:%M:%S",
                )
        df = df.sort_values("datetime").reset_index(drop=True)
        return df


if __name__ == "__main__":
    # TODO: Replace this resource_id with the correct one for UK demand
    DEMAND_RESOURCE_ID = "enter_the_correct_id_here"

    neso_fetcher = NesoDemandFetcher(resource_id=DEMAND_RESOURCE_ID)
    meta = neso_fetcher.get_resource_metadata(resource_id=DEMAND_RESOURCE_ID)

    # Get metadata (optional)
    print("Resource last modified:", meta.get("last_modified"))

    # Fetch recent demand data
    df_demand = neso_fetcher.fetch_demand_data(DEMAND_RESOURCE_ID, limit=5000)
    print(df_demand)
    print("Data range:", df_demand["datetime"].min(), "to", df_demand["datetime"].max())
