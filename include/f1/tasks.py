from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from minio.helpers import ObjectWriteResult
from io import BytesIO

import json
import requests
import pendulum
import time

from helpers.logger import logger
from helpers.minio import get_minio_client, _retrieve_data

BUCKET_NAME='formula1'


## Helper functions
def _get_meetings(ds):
    logger.info(f"date is {ds}")
    api = BaseHook.get_connection('f1_base_api')
    url = f"{api.host}/meetings?year={pendulum.from_format(ds, fmt='YYYY-MM-DD').year}"
    response = requests.get(url, headers=api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    return response.json()


def _get_most_recent_meeting(data):
    if not isinstance(data, list):
        raise AirflowFailException("Data is not of type list")

    if len(data) <= 0:
        raise AirflowFailException("Empty list")

    if data[-1]['meeting_key'] is not None:
        return data[-1]['meeting_key']

    logger.error(f"Input data to the task is: {data}")
    raise AirflowFailException("No Valid meeting_key found")


def _get_specific_meeting(data, meeting_key=None, meeting_name=None, meeting_location=None):
    logger.info(f"Input data type: {type(data)}")

    if not isinstance(data, list):
        raise AirflowFailException("Data is not of type list")

    if len(data) <= 0:
        raise AirflowFailException("Empty list")

    logger.info(f"Input data element type: {type(data[0])}")

    args = {k: v for k, v in locals().items() if k != "data" and v is not None}
    param, value = next(iter(args.items()))

    logger.info(f"Search Args is: {args}")
    logger.info(f"\nSearch Parameter is: {param} \n Search Parameter Value is: {value}")

    index = next((i for i, item in enumerate(data) if str(item.get(param)) == value), None)
    logger.info(f"Index is: {index}")

    if index is not None:
        return data[index]['meeting_key']

    logger.error(f"Input data to the task is: {data}")
    raise AirflowFailException("No Valid meeting_key found")


def _store_meetings(data):
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        logger.warning(f"Bucket: {BUCKET_NAME} does not exist. Attempting to create bucket")
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket: {BUCKET_NAME} created")

    encoded = json.dumps(data, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"meetings/meetings.json",
        data=BytesIO(encoded),
        length=len(encoded)
    )
    return f'{objw.bucket_name}/meetings'


def _get_sessions(meeting_key):
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/sessions?meeting_key={meeting_key}"
    response = requests.get(url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    return response.json()


def _store_sessions(meeting_key, data):
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        logger.warning(f"Bucket: {BUCKET_NAME} does not exist. Attempting to create bucket")
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket: {BUCKET_NAME} created")

    logger.info(f"Bucket {BUCKET_NAME} exists, continuing to store file")

    encoded = json.dumps(data, ensure_ascii=False).encode('utf8')
    prefix = f"sessions/{meeting_key}"
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{prefix}/session.json",
        data=BytesIO(encoded),
        length=len(encoded)
    )
    return f'{objw.bucket_name}/{prefix}/session.json'


def _get_drivers(meeting_key):
    time.sleep(40)
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/drivers?meeting_key={meeting_key}"
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.info(f"Response Data Type: {type(response.json())}")

    obj = _store_data(response.json(), 'drivers', f"drivers/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_pits(meeting_key):
    # https://api.openf1.org/v1/pit?meeting_key=1261
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/pit?meeting_key={meeting_key}"

    time.sleep(40)
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.info(f"Response Data Type: {type(response.json())}")

    obj = _store_data(response.json(), 'pit', f"pits/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_race_control(meeting_key):
    # https://api.openf1.org/v1/race_control?meeting_key=1261
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/race_control?meeting_key={meeting_key}"

    time.sleep(40)
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.info(f"Response Data Type: {type(response.json())}")

    obj = _store_data(response.json(), 'race_control', f"race_control/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_stints(meeting_key):
    # https://api.openf1.org/v1/stints?meeting_key=1260
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/stints?meeting_key={meeting_key}"

    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.info(f"Response Data Type: {type(response.json())}")

    obj = _store_data(response.json(), 'stints', f"stints/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_team_radio(meeting_key):
    # https://api.openf1.org/v1/team_radio?meeting_key=1260
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/team_radio?meeting_key={meeting_key}"

    time.sleep(40)
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.info(f"Response Data Type: {type(response.json())}")

    obj = _store_data(response.json(), 'team_radio', f"team_radio/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_weather(meeting_key):
    # https://api.openf1.org/v1/weather?meeting_key=1249
    pass


# get the following only for races, sprints and qualifying
def _get_positions(meeting_key, session_key, driver_number):
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/position?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(45)
    logger.debug(f"Making API Call to: {url}\n\n")
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.debug(f"Response Data Type: {type(response.json())}")

    obj = _store_data(data=response.json(), object_prefix=f"positions/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_locations(meeting_key, session_key, driver_number):
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/location?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(60)
    logger.debug(f"Making API Call to: {url}\n\n")
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.debug(f"Response Data Type: {type(response.json())}")

    obj = _store_data(data=response.json(), object_prefix=f"locations/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_intervals(meeting_key, session_key, driver_number):
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/intervals?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(50)
    logger.debug(f"Making API Call to: {url}\n\n")
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.debug(f"Response Data Type: {type(response.json())}")

    obj = _store_data(data=response.json(), object_prefix=f"intervals/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_car_data(meeting_key, session_key, driver_number, speed_threshold=0):
    # https://api.openf1.org/v1/car_data?meeting_key=1261&driver_number=44&session_key=9979
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/car_data?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}&speed>={speed_threshold}"

    time.sleep(50)
    logger.debug(f"Making API Call to: {url}\n\n")
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.debug(f"Response Data Type: {type(response.json())}")

    obj = _store_data(data=response.json(), object_prefix=f"car_data/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_laps(meeting_key, session_key, driver_number):
    # https://api.openf1.org/v1/laps?meeting_key=1261&driver_number=44&session_key=9979
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/laps?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(40)
    logger.debug(f"Making API Call to: {url}\n\n")
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        raise AirflowFailException(f"API call failed [{response.status_code}]: {response.text}")

    logger.debug(f"Response Data Type: {type(response.json())}")

    obj = _store_data(data=response.json(), object_prefix=f"laps/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _store_data(data, object_name: str, object_prefix: str, object_type: str='json') -> ObjectWriteResult:
    if isinstance(data, list) and len(data) <= 0:
        logger.warning(f"Data is empty, No data to store")

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        logger.warning(f"Bucket: {BUCKET_NAME} does not exist. Attempting to create bucket")
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket: {BUCKET_NAME} created")

    logger.info(f"Bucket {BUCKET_NAME} exists, continuing to store file")

    encoded = json.dumps(data, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{object_prefix}/{object_name}.{object_type}",
        data=BytesIO(encoded),
        length=len(encoded)
    )

    logger.info(
        "Created {0} object; etag: {1}, version-id: {2}".format(
            objw.object_name, objw.etag, objw.version_id,
        )
    )

    return objw


def _get_session_list(sessions):
    logger.info(f"input data type: {type(sessions)}")
    logger.info(f"input data: {sessions}")
    return [
        {"session_key": session['session_key'], "session_type": session['session_type']}
        for session in sessions
    ]


def _get_driver_list(session_keys_list, drivers_asset_path):
    # only for Qualifying or Race or Sprint
    valid_session_keys = []
    logger.debug(f"Input is {session_keys_list}\n\n")
    logger.info(f"checking the following sessions: {session_keys_list}")

    for item in session_keys_list:
        logger.debug(f"Item is: {item}")
        if item.get("session_type") in ('Qualifying', 'Race'):
            logger.info(f"Checking {item}")
            valid_session_keys.append(item.get("session_key"))

    if len(valid_session_keys) <= 0:
        logger.error(f"Input data to the task is: {session_keys_list}")
        raise AirflowFailException("No Qualifying or Race or Sprint session for the meeting")

    logger.debug(f"Valid session list: {valid_session_keys}")

    driver_data = _retrieve_data(full_object_name=drivers_asset_path)
    logger.info(f"driver_data: {driver_data}")

    if driver_data is not None:
        return [
            {
                "meeting_key": driver["meeting_key"],
                "session_key": driver["session_key"],
                "driver_number": driver["driver_number"],
            }
            for driver in driver_data
            if driver["session_key"] in valid_session_keys
        ]
    else:
        raise AirflowFailException("Error fetching driver data for relevant sessions")


if __name__ == '__main__':
    object_name = 'drivers/1262/drivers.json'
