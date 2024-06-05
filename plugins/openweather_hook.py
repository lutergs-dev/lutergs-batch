import json
import urllib.parse

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class OpenWeatherLocationInfoHook(BaseHook):
    """
    Interact with OpenWeather API
    """
    _conn_id = "openweather-connection"

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
        self._latitude = latitude
        self._longitude = longitude
        self._units = units
        self._lang = lang

    def get_conn(self) -> requests.Response:
        """
        Get weather info from OpenWeather API
        :return:
        """
        http_hook: HttpHook = HttpHook(http_conn_id=self._conn_id, method="GET")
        extra_str = http_hook.get_connection(self._conn_id).get_extra()
        extra_json = json.loads(extra_str)
        token = extra_json["token"]

        params = {
            "appid": token,
            "lat": "{:.2f}".format(self._latitude),
            "lon": "{:.2f}".format(self._longitude),
            "units": self._units,
            "lang": self._lang
        }
        param_str = urllib.parse.urlencode(params)

        return http_hook.run(endpoint=f"/data/3.0/onecall?{param_str}")
