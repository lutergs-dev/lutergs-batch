import json

from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class OpenWeatherLocationInfoHook(BaseHook):
    """
    Interact with OpenWeather API
    """
    conn_id = "openweather-connection"

    def __init__(self,
                 latitude: float,
                 longitude: float,
                 units: str = "metric",
                 lang: str = "kr",
                 *args, **kwargs):
        """
        init OpenWeather Hook
        :param latitude: latitude of the location
        :param longitude: longitude of the location
        :param units: metric units. default to meter (metric)
        :param lang: language code. default to kr (korean)
        """
        super().__init__(*args, **kwargs)
        self.latitude = latitude
        self.longitude = longitude
        self.units = units
        self.lang = lang

    def get_conn(self) -> dict:
        """
        Get weather info from OpenWeather API
        :return:
        """
        http_hook: HttpHook = HttpHook(http_conn_id=self.conn_id, method="GET")
        extra_str = http_hook.get_connection(self.conn_id).get_extra()
        extra_json = json.loads(extra_str)
        token = extra_json["token"]

        return http_hook.run(
            endpoint=f'/data/3.0/onecall?'
                     f'appid={token}'
                     f'&lat{"{:.2f}".format(self.latitude)}'
                     f'&lon={"{:.2f}".format(self.longitude)}'
                     f'&units={self.units}'
                     f'&lang={self.lang}',
        )