from shutil import ExecError
import socket
import ssl
import platform
from datetime import datetime
import time
from pyais import decode
import json
import sys, traceback
import boto3
from botocore.config import Config
import certifi
import os
import logging
from multiprocessing import Pool

# Context creation
sslContext              = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
sslContext.verify_mode  = ssl.CERT_REQUIRED

# Setup logger - https://docs.python.org/3/library/logging.html#levels
logLevel = os.environ.get("LOGLEVEL", logging.INFO)
logfmt = '[%(levelname)s] %(asctime)s - %(message)s'
logging.basicConfig(level=logLevel, format=logfmt)

supportedAISmsgTypes = ['1','2','3','4','5','18']

session = boto3.Session()
write_client = session.client('timestream-write', config=Config(region_name="eu-west-1",read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

def get_attributes(msg_body):
    if msg_body['msg_type'] in [1,2,3,4,18]:
        return {
            'Dimensions': [
                {
                    'Name': 'mmmsi',
                    'Value': str(msg_body['mmsi']),
                    'DimensionValueType': 'VARCHAR'
                }
            ],
            'MeasureName': 'position',
            'MeasureValueType': 'MULTI'
        }
    if msg_body['msg_type'] == 5:
        return {
            'Dimensions': [
                {
                    'Name': 'mmmsi',
                    'Value': str(msg_body['mmsi']),
                    'DimensionValueType': 'VARCHAR'
                },
                {
                    'Name': 'imo',
                    'Value': str(msg_body['imo']),
                    'DimensionValueType': 'VARCHAR'
                },
                {
                    'Name': 'name',
                    'Value': str(msg_body['shipname']),
                    'DimensionValueType': 'VARCHAR'
                },
                {
                    'Name': 'callsign',
                    'Value': str(msg_body['callsign']),
                    'DimensionValueType': 'VARCHAR'
                },
                {
                    'Name': 'ship_type',
                    'Value': str(msg_body['ship_type']),
                    'DimensionValueType': 'VARCHAR'
                },
            ],
            'MeasureName': 'status',
            'MeasureValueType': 'MULTI'
        }
    return {}

def get_measures(msg_body): 
    """
    Returns measure by msg type
    """
    if msg_body['msg_type'] in [1,2,3]:
        return [
            {
                'Name': 'longitude',
                'Value': str(msg_body['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(msg_body['lat']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'speed',
                'Value': str(msg_body['speed']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'course',
                'Value': str(msg_body['course']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'turn',
                'Value': str(msg_body['turn']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'status',
                'Value': str(msg_body['status']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'maneuver',
                'Value': str(msg_body['maneuver']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'heading',
                'Value': str(msg_body['heading']),
                'Type': 'BIGINT'
            },
        ]
    if msg_body['msg_type'] == 4:
        return [
            {
                'Name': 'longitude',
                'Value': str(msg_body['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(msg_body['lat']),
                'Type': 'DOUBLE'
            },
        ]
    if msg_body['msg_type'] == 5:
        measures = [
            {
                'Name': 'destination',
                'Value': str(msg_body['destination']),
                'Type': 'VARCHAR'
            },
            {
                'Name': 'to_bow',
                'Value': str(msg_body['to_bow']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'to_stern',
                'Value': str(msg_body['to_stern']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'to_port',
                'Value': str(msg_body['to_port']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'to_starboard',
                'Value': str(msg_body['to_starboard']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'draught',
                'Value': str(msg_body['draught']),
                'Type': 'DOUBLE'
            },
        ]
        if msg_body['destination'] != "":
            measures.insert(0, {
                'Name': 'destination',
                'Value': str(msg_body['destination']),
                'Type': 'VARCHAR'
            })
        return measures
    if msg_body['msg_type'] == 18:
        return [
            {
                'Name': 'longitude',
                'Value': str(msg_body['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(msg_body['lat']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'speed',
                'Value': str(msg_body['speed']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'course',
                'Value': str(msg_body['course']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'heading',
                'Value': str(msg_body['heading']),
                'Type': 'BIGINT'
            },
        ]
    return []

def get_timestream_table(msg_type):
    if msg_type in [1,2,3,4,18]:
        return "Positions"
    if msg_type == 5:
        return "Status"
    return ""
    

def put_timestream(msg_body):
    """
    Inserts msg to timestream
    """
    logging.debug(msg_body)
    now = round(time.time())
    records = [{
        'Time': str(now),
        'TimeUnit': 'SECONDS',
        'Version': now,
        'MeasureValues': get_measures(msg_body)
    }]
    try:
        logging.debug(f"Writing to {get_timestream_table(msg_body['msg_type'])}")
        logging.debug(records)
        return write_client.write_records(DatabaseName='AIS', TableName=get_timestream_table(msg_body['msg_type']), Records=records, CommonAttributes=get_attributes(msg_body))
    except write_client.exceptions.RejectedRecordsException as err:
        logging.error(f"RejectedRecords: {err}")
        for rr in err.response["RejectedRecords"]:
            logging.error(f"Rejected Index {str(rr['RecordIndex'])}:{rr['Reason']}")
            if 'ExistingVersion' in rr:
                logging.error(f"Rejected record existing version: {rr['ExistingVersion']}")
    except Exception as err:
        logging.info(f" Error occurred on {msg_body}")
        logging.error(err)


def stream_message(msg_body):
    """
    Filters & preps for Timestream.
    """
    try:
        msg_body['status'] = msg_body['status'].decode("utf-8")
        msg_body['status'] = msg_body['status'].split('.')[1].split(':')[0]
    except:
        pass
    try:
        msg_body['maneuver'] = msg_body['maneuver'].decode("utf-8")
        msg_body['maneuver'] = msg_body['maneuver'].split('.')[1].split(':')[0]
    except:
        pass
    if msg_body['callsign'] == "":
        msg_body['callsign'] = "-"
    try:
        put_response = put_timestream(msg_body)
    except Exception as err:
        logging.error(f"Kinesis - Error Msg: {err} - Processing: {str(msg_body)}")
        raise Exception
    else:
        return put_response


def checksum(sentence):
    import re
    if re.search("\n$", sentence):
        sentence = sentence[:-1]
    nmeadata,cksum = re.split('\*', sentence)
    calc_cksum = 0
    for s in nmeadata:
        calc_cksum ^= ord(s)
    result = str(hex(calc_cksum)).split("x")[1]
    return result


def create_multi_part(parts):
    decoded_parts = []
    for part in parts:
        if '!' in part:
            data = part.split("!")
            string_to_decode = '!' + str(data[1])
        else:
            string_to_decode = '!' + part
        string_to_decode = string_to_decode.replace("\r\n","")
        if "AIVDM" in string_to_decode:
            decoded_parts.append(string_to_decode)
    return decoded_parts

if __name__ == '__main__':
    while True:
        """
        Connect via SSL to Orbcomm websocket and decode msgs to fire to Kinesis streams.
        """
        logging.info('Orbcomm Ingester Script Starting ...')
        # Load the CA certificates used for validating the peer's certificate.
        sslContext.load_verify_locations(cafile=os.path.relpath(certifi.where()),capath=None, cadata=None)
        sslContext.check_hostname = False
        # Create an SSLSocket.                         
        clientSocket = socket.create_connection(("globalais2.orbcomm.net", 9054))
        secureClientSocket = sslContext.wrap_socket(clientSocket, do_handshake_on_connect=False, )
        # Only connect, no handshake.
        logging.debug('Established connection')
        # Explicit handshake.
        secureClientSocket.do_handshake()
        logging.debug("Complete SSL handshake")
        # Get the certificate of the server and print.
        serverCertificate = secureClientSocket.getpeercert()
        logging.debug("Certificate obtained from the server:")
        logging.debug(serverCertificate)
        # TODO move these to envVar
        user_name= "SSEH_Lss"
        password= "tpWB3UkPnoF2"
        try:
            nmea_string = "$PMWLSS,{},5,{},{},1*".format(time,user_name,password)
            chksum_string = nmea_string[1:]
            package = "{}{}\r\n".format(nmea_string, checksum(chksum_string))
            b = bytearray()
            b.extend(package.encode('utf-8'))
            secureClientSocket.send(b)
        except Exception as e:
            logging.warning(e)
            continue
        first_part = ""
        parts = []
        decoded_message = None
        try:
            pool = Pool()
            while True:
                try:
                    row = secureClientSocket.recv(4096)
                    if row !="":
                        row_to_string = row.decode("utf-8")
                        if row_to_string !="":
                            logging.debug(f"Raw - {row}")
                            logging.debug(f"Decoded utf-8 - {row_to_string}")
                            # Check for multipart msgs
                            if 'g:1-2-' in row_to_string:
                                first_part = row_to_string
                            else:
                                # Check message is not part of multipart msg.
                                if first_part == "":
                                    if '!' in row_to_string:
                                        data = row_to_string.split("!")
                                        string_to_decode = '!' + str(data[1])
                                    else:
                                        string_to_decode = '!' + row_to_string
                                    if string_to_decode.find("AIVDM") != -1:
                                        decoded_message = decode(string_to_decode).asdict()
                                        logging.debug(f"AIS Decoded First - {decoded_message}")
                                # Check that we are dealing with the second part of a multipart msg.
                                # N.B. This solution only deals with 2 part msgs atm.
                                if first_part !="" and 'g:2-2-' in row_to_string:
                                    logging.debug(f"First part  - {first_part}")
                                    logging.debug(f"Second part - {row_to_string}")
                                    parts = [first_part, row_to_string]
                                    multipart = create_multi_part(parts)
                                    logging.debug(f"Raw Multipart - {multipart}")
                                    decoded_message = decode(*multipart).asdict()
                                    logging.debug(f"AIS Decoded Multipart - {decoded_message}")
                                    first_part = ""
                                if decoded_message is not None:
                                    if str(decoded_message['msg_type']) in supportedAISmsgTypes:
                                        try:
                                            result = pool.apply_async(stream_message, [decoded_message])
                                            logging.debug(result.get(timeout=1))
                                        except Exception as error:
                                            logging.error(error)
                                            raise Exception
                except Exception as error:
                    logging.error(error)
                    raise Exception
        except Exception as error:
            logging.error(error)
            raise Exception
