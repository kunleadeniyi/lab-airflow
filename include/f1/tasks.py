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
def _get_meetings(base_api, ds):
    logger.info(f"date is {ds}")
    url = f"{base_api}/meetings?year={pendulum.from_format(ds, fmt='YYYY-MM-DD').year}"
    api = BaseHook.get_connection('f1_base_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    return json.dumps(response.json())


def _get_most_recent_meeting(data):
    data=json.loads(data)
    print(type(data))
    
    if (isinstance(data, list) == False):
        raise AirflowFailException("Data is not of type list")
    
    if (len(data) <= 0):
        raise AirflowFailException("Empty list")
    
    if data[-1]['meeting_key'] is not None:
        output = data[-1]['meeting_key'] 
    else:
        logger.error(f"Input data to the task is: {data}")
        raise AirflowFailException(f"This is a valid fail reason: \nNo Valid meeting_key found")
    
    return output


def _store_meetings(data):
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        logger.warning(f"Bucket: {BUCKET_NAME} does not exist. Attempting to create bucket")
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket: {BUCKET_NAME} created")

    data = json.loads(data)
    data = json.dumps(data, ensure_ascii=False).encode('utf8')

    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"meetings/meetings.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/meetings'

def _get_sessions(meeting_key):
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/sessions?meeting_key={meeting_key}"
    response = requests.get(url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    return json.dumps(response.json())

def _store_sessions(meeting_key, data):
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        logger.warning(f"Bucket: {BUCKET_NAME} does not exist. Attempting to create bucket")
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket: {BUCKET_NAME} created")

    logger.info(f"Bucket {BUCKET_NAME} exists, continuing to store file")

    data = json.loads(data)
    data = json.dumps(data, ensure_ascii=False).encode('utf8')
    prefix = f"sessions/{meeting_key}"
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{prefix}/session.json",
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/{prefix}/session.json'

def _get_drivers(meeting_key):

    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/drivers?meeting_key={meeting_key}"

    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    logger.info(f"Response Data: {response.json()}")
    logger.info(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data, 'drivers', f"drivers/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"
    


def _get_pits(meeting_key):
    # https://api.openf1.org/v1/pit?meeting_key=1261
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/pit?meeting_key={meeting_key}"

    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    logger.info(f"Response Data: {response.json()}")
    logger.info(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data, 'pit', f"pits/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"
    

def _get_race_control(meeting_key):
    # https://api.openf1.org/v1/race_control?meeting_key=1261
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/race_control?meeting_key={meeting_key}"

    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    logger.info(f"Response Data: {response.json()}")
    logger.info(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data, 'race_control', f"race_control/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"

def _get_stints(meeting_key):
    # https://api.openf1.org/v1/stints?meeting_key=1260
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/stints?meeting_key={meeting_key}"

    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    logger.info(f"Response Data: {response.json()}")
    logger.info(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data, 'stints', f"stints/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"

def _get_team_radio(meeting_key):
    # https://api.openf1.org/v1/team_radio?meeting_key=1260 
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/team_radio?meeting_key={meeting_key}"

    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    logger.info(f"Response Data: {response.json()}")
    logger.info(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data, 'team_radio', f"team_radio/{meeting_key}")
    return f"{BUCKET_NAME}/{obj.object_name}"

def _get_weather(meeting_key):
    # https://api.openf1.org/v1/weather?meeting_key=1249
    pass

# get the following only for races, sprints and qualifying
def _get_positions(meeting_key, session_key, driver_number):
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/position?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(15)
    logger.debug(f"Making API Call to: {url}\n\n")
    # return url
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    # logger.debug(f"Response Data: {response.json()}")
    logger.debug(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data=data, object_prefix=f"positions/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def _get_locations(meeting_key, session_key, driver_number):
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/location?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(30)
    logger.debug(f"Making API Call to: {url}\n\n")
    # return url
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    # logger.debug(f"Response Data: {response.json()}")
    logger.debug(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data=data, object_prefix=f"locations/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"

def _get_intervals(meeting_key, session_key, driver_number):
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/intervals?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(30)
    logger.debug(f"Making API Call to: {url}\n\n")
    # return url
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    # logger.debug(f"Response Data: {response.json()}")
    logger.debug(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data=data, object_prefix=f"intervals/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"

def  _get_car_data(meeting_key, session_key, driver_number, speed_threshold=0):
    # https://api.openf1.org/v1/car_data?meeting_key=1261&driver_number=44&session_key=9979
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/car_data?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}&speed>={speed_threshold}"

    time.sleep(30)
    logger.debug(f"Making API Call to: {url}\n\n")
    # return url
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    # logger.debug(f"Response Data: {response.json()}")
    logger.debug(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data=data, object_prefix=f"car_data/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"


def  _get_laps(meeting_key, session_key, driver_number):
    # https://api.openf1.org/v1/laps?meeting_key=1261&driver_number=44&session_key=9979
    base_api = BaseHook.get_connection('f1_base_api')
    logger.debug(f"Working on: Meeting key={meeting_key} \nSession Key={session_key} \nDriver Number={driver_number}")
    url = f"{base_api.host}/laps?meeting_key={meeting_key}&session_key={session_key}&driver_number={driver_number}"

    time.sleep(30)
    logger.debug(f"Making API Call to: {url}\n\n")
    # return url
    response = requests.get(url=url, headers=base_api.extra_dejson['headers'])

    if response.status_code != 200:
        logger.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    # logger.debug(f"Response Data: {response.json()}")
    logger.debug(f"Response Data Type: {type(response.json())}")

    data = json.dumps(response.json())
    obj = _store_data(data=data, object_prefix=f"laps/{meeting_key}/{session_key}", object_name=f"driver_number_{driver_number}")
    return f"{BUCKET_NAME}/{obj.object_name}"

def _store_data(data: json, object_name: str, object_prefix: str, object_type: str='json') -> ObjectWriteResult:

    if isinstance(data, list) == True:
        if len(data) <= 0:
            logger.warning(f"Data is empty, No data to store")

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        logger.warning(f"Bucket: {BUCKET_NAME} does not exist. Attempting to create bucket")
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket: {BUCKET_NAME} created")

    logger.info(f"Bucket {BUCKET_NAME} exists, continuing to store file")

    data = json.loads(data)
    data = json.dumps(data, ensure_ascii=False).encode('utf8')
    prefix = object_prefix
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{prefix}/{object_name}.{object_type}",
        data=BytesIO(data),
        length=len(data)
    )

    logger.info(print(
        "Created {0} object; etag: {1}, version-id: {2}".format(
            objw.object_name, objw.etag, objw.version_id,
        ),
    ))

    return objw


def _get_session_list(sessions):
    logger.info(f"input data: {sessions}")
    data = json.loads(sessions)
    logger.info(f"\n\ninput data type: {type(data)}")
    logger.info(f"\n\ntransformed json.loads data: {data}")
    output = {"data": [{ "session_key": session['session_key'], "session_type": session['session_type'] } for session in data]}
    return json.dumps(output)

def _get_driver_list(session_keys_list, drivers_asset_path):
    # only for Qualifying or Race or Sprint
    valid_session_keys = []
    logger.debug(f"Input is {session_keys_list}\n\n")

    session_keys = json.loads(session_keys_list)['data']
    logger.debug(f"checking the following sessions: {session_keys}")
    logger.debug(f"session_keys_list data type: {type(session_keys)}")

    for item in list(session_keys):
        logger.debug(f"Item is: {item}")

        if item.get("session_type") in ('Qualifying', 'Race'): #('Qualifying', 'Race' , 'Sprint'):
             logger.debug(f"Checking {item}")
             valid_session_keys.append(item.get("session_key"))

    if len(valid_session_keys) <= 0:
        logger.error(f"Input data to the task is: {session_keys_list}")
        raise AirflowFailException("No Qualifying or Race or Sprint session for the meeting")
    
    logger.debug(f"Valid session list: {valid_session_keys}")
    # ideas:
        # 1. read drivers asset where to get the drivers list 
    # 2. or call the drivers api 
    # 3. or push the drivers to an xcom and pull from it 

    # using idea 1
    # driver_data = _retrieve_data(bucket_name=BUCKET_NAME, object_name=drivers_asset_path)
    driver_data = _retrieve_data(full_object_name=drivers_asset_path)
    logger.debug(f"driver_data: {driver_data}")

    if driver_data is not None:
        driver_list = [
            [ driver["meeting_key"],  driver["session_key"], driver["driver_number"] ]
            for driver in driver_data
            if driver["session_key"] in valid_session_keys
        ]
        
        return driver_list
    else:
        raise AirflowFailException("Error fetching driver data for relevant sessions")

# class MinioHelpers:

#     def _retrieve_data(bucket_name=BUCKET_NAME, object_name=None): # move to Class to use overloading
#         client =  get_minio_client()

#         if object_name == None:
#             raise Exception("object_name cannot be Empty")
        
#         try: 
#             response = client.get_object(
#                 bucket_name=bucket_name, 
#                 object_name=object_name
#             )

#             data = response.read()
#             json_data = json.loads(data)
            
#             logger.debug("Type is: {0}".format(type(json_data)))
#             return json_data
            
#         except Exception as e:
#             logger.error(f"Exception Block: \nType: {type(e)} \nRepr: {repr(e)}")
#         finally:
#             pass
#             # response.close()
#             # response.release_conn()
        

#     def _retrieve_data(full_object_name=None): # move to Class to use overloading
#         client =  get_minio_client()

#         if full_object_name == None:
#             raise Exception("object_name cannot be Empty")
        
#         try: 
#             response = client.get_object(
#                 bucket_name=full_object_name.split('/')[0], 
#                 object_name='/'.join(full_object_name.split('/')[1:])
#             )

#             data = response.read()
#             json_data = json.loads(data)
            
#             logger.debug("Type is: {0}".format(type(json_data)))
#             return json_data
            
#         except Exception as e:
#             logger.error(f"Exception Block: \nType: {type(e)} \nRepr: {repr(e)}")
#         finally:
#             logger.debug("retrieve data from minion function called ")
#             pass
#             # response.close()
#             # response.release_conn()

if __name__ == '__main__':
    object_name = 'drivers/1262/drivers.json'

    # print(_retrieve_data(bucket_name=BUCKET_NAME, object_name=object_name))
