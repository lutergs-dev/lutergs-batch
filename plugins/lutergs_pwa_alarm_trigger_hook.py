import json
from typing import Union

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class LuterGSPwaAlarmHook(BaseHook):
    """
    Interact with LuterGS Pwa alarm trigger
    """
    conn_id = "lutergs-pwa-connection"

    class LuterGSPwaAlarmRequest:

        def __init__(self, title: str, message: str, image_url: Union[str, None]):
            self.title = title
            self.message = message
            self.image_url = image_url

        def to_json(self):
            if self.image_url is not None:
                data = {
                    "title": self.title,
                    "message": self.message,
                    "imageUrl": self.image_url
                }
            else:
                data = {
                    "title": self.title,
                    "message": self.message
                }
            return json.dumps(data)

    def __init__(self,
                 topic_uuid: str,
                 alarm_request: LuterGSPwaAlarmRequest,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic_uuid = topic_uuid
        self.alarm_request = alarm_request

    def get_conn(self) -> dict:
        # extra 에 token 이 있다고 가정
        http_hook: HttpHook = HttpHook(http_conn_id='lutergs_pwa_alarm_hook')
        extra_str = http_hook.get_connection(self.conn_id).get_extra()
        extra_json = json.loads(extra_str)
        token = extra_json["token"]

        requests.Request()

        return http_hook.run(
            endpoint=f'/push/topics/{self.topic_uuid}',
            method="POST",
            headers={
                'Authorization': token,
                'Content-Type': 'application/json'
            },
            data=self.alarm_request.to_json()
        )
