from minio import Minio
from airflow.hooks.base import BaseHook
from helpers.logger import logger

BUCKET_NAME='formula1'

import json
# def get_minio_client():
#     minio = BaseHook.get_connection('minio').extra_dejson
#     client = Minio(
#         endpoint=minio['endpoint_url'].split('//')[1],
#         access_key=minio['aws_access_key_id'],
#         secret_key=minio['aws_secret_access_key'],
#         secure=False
#     )
#     return client

def get_minio_client():
    # get minio client
    minio = BaseHook.get_connection('minio')

    if minio.host is None:
        host = minio.extra_dejson['host']
    else:
        host = minio.host

    logger.info(f"Got MinIO host: Hostname is - {host}")
    logger.info(f"Connection to MinIO host as user - {minio.login}")
    client = Minio(
        endpoint=host.split('//')[1],
        access_key=minio.login,
        secret_key=minio.password, 
        secure=False
    )
    return client

def _retrieve_data(full_object_name=None): # move to Class to use overloading
    client =  get_minio_client()

    if full_object_name == None:
        raise Exception("object_name cannot be Empty")
    
    try: 
        response = client.get_object(
            bucket_name=full_object_name.split('/')[0], 
            object_name='/'.join(full_object_name.split('/')[1:])
        )

        data = response.read()
        json_data = json.loads(data)
        
        logger.debug("Type is: {0}".format(type(json_data)))
        return json_data
        
    except Exception as e:
        logger.error(f"Exception Block: \nType: {type(e)} \nRepr: {repr(e)}")
    finally:
        logger.debug("retrieve data from minion function called ")
        pass
        # response.close()
        # response.release_conn()

# class MinioHelpers:

#     @staticmethod
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
        
#     @staticmethod
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
