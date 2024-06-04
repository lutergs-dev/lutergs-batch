import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.date_time import DateTimeSensorAsync

from openweather_hook import OpenWeatherLocationInfoHook
from lutergs_pwa_alarm_trigger_hook import LuterGSPwaAlarmHook


def get_nearest_weather(openweather_response: dict):
    sunset_epoch_second = openweather_response["current"]["sunset"]
    hourly = openweather_response["hourly"]
    diff = sunset_epoch_second
    closest_hour_forecast = hourly[0]
    for hour in hourly:
        temp_diff = abs(hour["dt"] - sunset_epoch_second)
        if temp_diff < diff:
            diff = temp_diff
            closest_hour_forecast = hour
        else:
            break
    return closest_hour_forecast["weather"]



@dag(
    dag_id="sunset-alarmer",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    description="sunset trigger alarm",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="0 16 * * *",
    catchup=False,
    tags=["lutergs"]
)
def operator():

    @task(task_id="set_sunset_wait_time")
    def _set_sunset_wait_time(ti=None):
        hook = OpenWeatherLocationInfoHook(latitude=37.56, longitude=127.00)
        weather_response = hook.get_conn().json()

        sunset_epoch_second = weather_response["current"]["sunset"]

        sunset_datetime = datetime.datetime.fromtimestamp(sunset_epoch_second, datetime.timezone.utc) - datetime.timedelta(seconds=2)
        sunset_datetime_minus_2_minute = sunset_datetime - datetime.timedelta(minutes=2)

        ti.xcom_push(key="sunset_datetime", value=sunset_datetime)
        ti.xcom_push(key="sunset_datetime_minus_2_minute", value=sunset_datetime_minus_2_minute)

    set_sunset_wait_time = _set_sunset_wait_time()

    wait_until_sunset_minus_2_minute = DateTimeSensorAsync(
        task_id="wait_until_sunset_minus_2_minute",
        target_time='{{ task_instance.xcom_pull(task_ids="set_sunset_info", key="sunset_datetime_minus_2_minute") }}'
    )

    wait_until_sunset = DateTimeSensorAsync(
        task_id="wait_until_sunset",
        target_time='{{ task_instance.xcom_pull(task_ids="set_sunset_info", key="sunset_datetime") }}',
        poke_interval=datetime.timedelta(seconds=30)
    )

    @task(task_id="set_sunset_forecast")
    def _set_current_forecast(ti=None):
        hook = OpenWeatherLocationInfoHook(latitude=37.56, longitude=127.00)
        weather_response = hook.get_conn().json()
        current_weather_forecast = weather_response["current"]["weather"][0]

        ti.xcom_push(key="current_forecast", value=current_weather_forecast)

    set_current_forecast = _set_current_forecast()

    @task(task_id="trigger_sunset_alarm")
    def _trigger_sunset_alarm(ti=None):
        sunset_alarm_id = Variable.get("LUTERGS_PWA_SUNSET_TOPIC", deserialize_json=False)

        current_weather = ti.xcom_pull(task_ids="set_current_forecast", key="current_forecast")

        hook = LuterGSPwaAlarmHook(
            topic_uuid=sunset_alarm_id,
            alarm_request=LuterGSPwaAlarmHook.LuterGSPwaAlarmRequest(
                title="일출입니다!",
                message=f"해가 떠오릅니다. 지금은 {current_weather["description"]} 날씨입니다.",
                image_url=f'https://openweathermap.org/img/wn/{current_weather["icon"]}@2x.png'
            )
        )
        hook.get_conn()

    trigger_sunset_alarm = _trigger_sunset_alarm()

    set_sunset_wait_time >> wait_until_sunset_minus_2_minute
    wait_until_sunset_minus_2_minute >> [wait_until_sunset, set_current_forecast]
    wait_until_sunset >> trigger_sunset_alarm


operator()
