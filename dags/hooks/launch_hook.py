import requests
from airflow.hooks.base_hook import BaseHook


class LaunchHook(BaseHook):
    def __init__(self):
        super().__init__(source=None)

        
    def fetch(self, start_date, end_date, **kwargs):
        query = f"https://launchlibrary.net/1.4/launch?startdate={start_date}&enddate={end_date}"
        response = requests.get(query)
        print(query)
        return response.json()['launches']


