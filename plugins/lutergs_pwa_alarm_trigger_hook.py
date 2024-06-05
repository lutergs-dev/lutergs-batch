import json
from typing import Union

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class LuterGSPwaAlarmHook(BaseHook):
    """
    Interact with LuterGS Pwa alarm trigger
    """
    _conn_id = "lutergs-pwa-connection"

    class LuterGSPwaAlarmRequest:

        def __init__(self, title: str, message: str, image_url: Union[str, None] = None):
            self._title = title
            self._message = message
            self._image_url = image_url

        def to_json(self):
            if self._image_url is not None:
                data = {
                    "title": self._title,
                    "message": self._message,
                    "imageUrl": self._image_url
                }
            else:
                data = {
                    "title": self._title,
                    "message": self._message
                }
            return json.dumps(data)

    def __init__(self,
                 topic_uuid: str,
                 alarm_request: LuterGSPwaAlarmRequest,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._topic_uuid = topic_uuid
        self._alarm_request = alarm_request

    def get_conn(self) -> requests.Response:
        # extra 에 token 이 있다고 가정
        http_hook: HttpHook = HttpHook(http_conn_id=self._conn_id, method="POST")
        extra_str = http_hook.get_connection(self._conn_id).get_extra()
        extra_json = json.loads(extra_str)
        token = extra_json["token"]

        return http_hook.run(
            endpoint=f'/push/topics/{self._topic_uuid}',
            headers={
                'Authorization': token,
                'Content-Type': 'application/json'
            },
            data=self._alarm_request.to_json()
        )
