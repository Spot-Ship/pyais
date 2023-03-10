import socket
import ssl
import time
from pyais import decode
import boto3
from botocore.config import Config
import certifi
from os import environ, path
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# Context creation
ssl_context              = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.verify_mode  = ssl.CERT_REQUIRED

# Setup logger - https://docs.python.org/3/library/logging.html#levels
log_level = environ.get("LOGLEVEL", logging.INFO)
log_format = '[%(levelname)s] %(asctime)s - %(message)s'
logging.basicConfig(level=log_level, format=log_format)

supported_msg_types = ['1','2','3','4','5','18','19','27']

session = boto3.Session()
kinesis_client = boto3.client("kinesis", region_name='eu-west-2')

def get_eta(message):
    """
    Deals with the dirty data from AIS to create an eta timestamp
    """
    if message['month'] > 12:
        message['month'] = 0
    if message['day'] > 31:
        message['day'] = 0
    if message['hour'] >= 24:
        message['hour'] = 0
    if message['minute'] >= 60:
        message['minute'] = 0
        
    eta = datetime.today() + relativedelta(months=message['month'], days=message['day'], hours=message['hour'], minutes=message['minute'])
    return str(int(eta.timestamp()))
            

def trimMessageForKinesis(message):
    if message['msg_type'] in [1,2,3]:
        return { 
            'msg_type': message['msg_type'],
            'mmsi': message['mmsi'],
            'lon': message['lon'],
            'lat': message['lat'],
            'speed': message['speed'],
            'course': message['course'],
            'turn': message['turn'],
            'status': message['status'],
            'maneuver': message['maneuver'],
            'heading': message['heading'],
            'time': message['time'],
            '@type': 'position',
        }
    if message['msg_type'] == 4:
        return {
            'msg_type': message['msg_type'],
            'mmsi': message['mmsi'],
            'lon': message['lon'],
            'lat': message['lat'],
            'time': message['time'],
            '@type': 'position',
        }
    if message['msg_type'] == 5:
        return {
            'msg_type': message['msg_type'],
            'mmsi': message['mmsi'],
            'imo': message['imo'],
            'ship_type': message['ship_type'],
            'shipname': message['shipname'],
            'callsign': message['callsign'],
            'to_bow': message['to_bow'],
            'to_stern': message['to_stern'],
            'to_port': message['to_port'],
            'to_starboard': message['to_starboard'],
            'draught': message['draught'],
            'eta': message['eta'],
            'destination': message['destination'],
            'time': message['time'],
            '@type': 'status',
        }
    if message['msg_type'] in [18,19]:
        return {
            'msg_type': message['msg_type'],
            'mmsi': message['mmsi'],
            'lon': message['lon'],
            'lat': message['lat'],
            'speed': message['speed'],
            'course': message['course'],
            'heading': message['heading'],
            'time': message['time'],
            '@type': 'position',
        }
    if message['msg_type'] == 27:
        return {
            'msg_type': message['msg_type'],
            'mmsi': message['mmsi'],
            'lon': message['lon'],
            'lat': message['lat'],
            'speed': message['speed'],
            'course': message['course'],
            'status': message['status'],
            'time': message['time'],
            '@type': 'position',
        }
    return {}
    

def write_data_to_kinesis(messages):
    """
    Writes message to Kinesis stream
    """
    put_response = kinesis_client.put_records(
        StreamName='Orbcomm-AIS',
        Records=messages,
    )
    if put_response['FailedRecordCount'] > 0:
        logging.error(f"Failed to process {put_response['FailedRecordCount']}")
    return put_response


def get_type(msg_type):
    """
    Get which type the message is deserialised as
    """
    if msg_type in [1,2,3,4,18,19,27]:
        return "position"
    if msg_type == 5:
        return "status"
    return ""


def prep_message_for_kinesis(message):
    """
    Filters & preps for Kinesis.
    """
    try:
        message['time'] = round(time.time())
    except:
        pass
    try:
        message['@type'] = get_type(message['msg_type'])
    except:
        pass
    if message['msg_type'] in [1,2,3,27]:
        try:
            message['status'] = int(f"{message['status']}")
        except:
            pass
        if message['msg_type'] in [1,2,3]:
            try:
                message['maneuver'] = int(f"{message['maneuver']}")
            except:
                pass
            try:
                if (message['turn']) == -0.0:
                    message['turn'] = 0.0
                if (f"{message['turn']}" == "TurnRate.NO_TI_DEFAULT"):
                    message['turn'] = -128.0
                elif (f"{message['turn']}" == "TurnRate.NO_TI_LEFT"):
                    message['turn'] = -127.0
                elif (f"{message['turn']}" == "TurnRate.NO_TI_RIGHT"):
                    message['turn'] = 127.0
                elif ("TurnRate.NO_TI_" in f"{message['turn']}"):
                    message['turn'] = 0.0
            except:
                pass
    if message['msg_type'] in [5]:
        try:
            message['eta'] = get_eta(message)
        except:
            pass
        try:
            message['ship_type'] = int(f"{message['ship_type']}")
        except:
            pass
    return {
        "Data" : json.dumps(trimMessageForKinesis(message)),
        "PartitionKey": f"${message['mmsi']}${message['msg_type']}${message['time']}",
    }


def filter_messages(message):
    """
    Checks decoded message is of a type we care about & sends it to process it.
    """
    if message is None or str(message['msg_type']) in supported_msg_types:
        return None
    try:
        return prep_message_for_kinesis(message)
    except Exception as error:
        logging.error(error)
        raise Exception 
    


def prep_message_for_decoding(message):
    if '!' in message:
        message_parts = message.split("!")
        return '!' + str(message_parts[1])
    else:
        return '!' + message


def decode_single_part_message(message):
    """
    Decode single part AIS Messages
    """
    prepped_message = prep_message_for_decoding(message)
    if "AIVDM" in prepped_message:
        try:
            decoded_message = decode(prepped_message).asdict()
            logging.debug(f"AIS Decoded First - {decoded_message}")
            return decoded_message
        except Exception as error:
            logging.error(f"Error occured decoding: {message} | Error Message | {error}") 


def prep_multipart_message_for_decoding(part):
    return prep_message_for_decoding(part).replace("\r\n","")

def is_valid_multipart_part(part):
    return "AIVDM" in part
               
def decode_multipart_message(parts):
    """
    Decode multi-part AIS Messages
    """
    try:
        multipart_message = filter(is_valid_multipart_part, map(prep_multipart_message_for_decoding, parts))
        logging.debug(f"Raw Multipart - {multipart_message}")
        decoded_ais_message = decode(*multipart_message).asdict()
        logging.debug(f"AIS Decoded Multipart - {decoded_ais_message}")
        return decoded_ais_message
    except Exception as error:
        logging.error(f"Error occured decoding: {parts} | Error Message | {error}")


def checksum(nmea_string):
    """
    Create Checksum from nmea_string
    """
    import re
    if re.search("\n$", nmea_string):
        nmea_string = nmea_string[:-1]
    nmea_data,_ = re.split('\*', nmea_string)
    calculated_checksum = 0
    for s in nmea_data:
        calculated_checksum ^= ord(s)
    result = str(hex(calculated_checksum)).split("x")[1]
    return result
    

def get_orbcomm_socket():
    """
    Connect via SSL to Orbcomm websocket
    """
    # Load the CA certificates used for validating the peer's certificate.
    ssl_context.load_verify_locations(cafile=path.relpath(certifi.where()),capath=None, cadata=None)
    ssl_context.check_hostname = False
    # Create an SSLSocket.                         
    client_socket = socket.create_connection(("globalais2.orbcomm.net", 9054))
    secure_client_socket = ssl_context.wrap_socket(client_socket, do_handshake_on_connect=False, )
    # Only connect, no handshake.
    logging.debug('Established connection')
    # Explicit handshake.
    secure_client_socket.do_handshake()
    logging.debug("Complete SSL handshake")
    # Get the certificate of the server and print.
    # server_certificate = secure_client_socket.getpeercert()
    # logging.debug("Certificate obtained from the server:")
    # logging.debug(server_certificate)
    # TODO move these to envVar
    username= "SSEH_Lss"
    password= "tpWB3UkPnoF2"
    try:
        nmea_string = "$PMWLSS,{},5,{},{},1*".format(time,username,password)
        checksum_string = nmea_string[1:]
        package = "{}{}\r\n".format(nmea_string, checksum(checksum_string))
        bytes = bytearray()
        bytes.extend(package.encode('utf-8'))
        secure_client_socket.send(bytes)
    except Exception as error:
        logging.warning(error)
    return secure_client_socket


if __name__ == '__main__':
    """
    Main Thread:
        Connects to Orbcomm websocket, 
        Decodes AIS messages,
        Sends relevant data to Kinesis.
    """
    while True:
        logging.info('Orbcomm Ingester Script Starting ...')
        socket = get_orbcomm_socket() 
        try:
            first_part_of_multipart_message, multipart_message = "", []
            start_time = time_of_first_encountered_empty_message = time_to_throw_an_exception = datetime.today();
            message_counter, empty_message_counter, messages = 0, 0, []
            # Loop through messages from Orbcomm Stream
            while True:
                try:
                    raw_message = socket.recv(4096)
                    if raw_message == "":
                        continue
                    
                    encoded_message = raw_message.decode("utf-8")
                    if encoded_message =="":
                        empty_message_counter +=1
                        if empty_message_counter == 1:
                            time_of_first_encountered_empty_message = datetime.today();
                            time_to_throw_an_exception = time_of_first_encountered_empty_message + timedelta(seconds=5)
                        if empty_message_counter > 1 and time_to_throw_an_exception < datetime.today():
                            # Clean up backlog before erroring out
                            if len(messages) > 0:
                                write_data_to_kinesis(messages)
                                messages = []
                            raise Exception(f"Something interrupted the stream since: {time_of_first_encountered_empty_message}")
                        continue
                    
                    empty_message_counter = 0
                    message_counter +=1
                    if message_counter % 10000 == 0:
                        now = datetime.today()
                        interval = now - start_time
                        logging.info(f"Last 10,000 messages processed in {round(interval.total_seconds(), 2)} seconds | Rate: {round(10000/interval.total_seconds(), 2)} message per second | Processed {message_counter} so far.")
                        start_time = now

                    logging.debug(f"Raw - {raw_message}")
                    logging.debug(f"Decoded utf-8 - {encoded_message}")
                    
                    # Check for multipart msgs
                    # N.B. This solution only deals with 2 part msgs.
                    if 'AIVDM,2,1' in encoded_message:
                        first_part_of_multipart_message = encoded_message
                        continue
                    
                    # Check that we are dealing with the second part of a multipart msg.
                    # N.B. This solution only deals with 2 part msgs.
                    if first_part_of_multipart_message != "" and 'AIVDM,2,2' in encoded_message:
                        logging.debug(f"First part  - {first_part_of_multipart_message}")
                        logging.debug(f"Second part - {encoded_message}")
                        multipart_message = [first_part_of_multipart_message, encoded_message]
                        decoded = decode_multipart_message(multipart_message)
                        first_part_of_multipart_message = ""
                    else:
                        decoded = decode_single_part_message(encoded_message)
                    
                    prepped = filter_messages(decoded)
                    if prepped is not None:
                        messages.append(prepped)
                    
                    #N.B. 500 is the max supported
                    if len(messages) >= 400:
                        write_data_to_kinesis(messages)
                        messages = []
                    continue
                except Exception as error:
                    logging.error(error)
                    raise error
        except Exception as error:
            logging.error(error)
            raise error
