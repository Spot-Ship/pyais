import socket
import ssl
import time
from pyais import decode
import boto3
from botocore.config import Config
import certifi
import os
import logging
from multiprocessing import Pool
from datetime import datetime
from dateutil.relativedelta import relativedelta

multiprocessing_is_enabled = False

# Context creation
ssl_context              = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.verify_mode  = ssl.CERT_REQUIRED

# Setup logger - https://docs.python.org/3/library/logging.html#levels
log_level = os.environ.get("LOGLEVEL", logging.INFO)
log_format = '[%(levelname)s] %(asctime)s - %(message)s'
logging.basicConfig(level=log_level, format=log_format)

supported_msg_types = ['1','2','3','4','5','18','19','27']

session = boto3.Session()
write_client = session.client('timestream-write', config=Config(region_name="eu-west-1",read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

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
            

def get_attributes(message):
    """
    Returns attribute by msg_type
    """
    if message['msg_type'] in [1,2,3,4,18,19,27]:
        return {
            'Dimensions': [
                {
                    'Name': 'mmmsi',
                    'Value': str(message['mmsi']),
                    'DimensionValueType': 'VARCHAR'
                }
            ],
            'MeasureName': 'position',
            'MeasureValueType': 'MULTI'
        }
    if message['msg_type'] == 5:
        dimensions = [
            {
                'Name': 'mmmsi',
                'Value': str(message['mmsi']),
                'DimensionValueType': 'VARCHAR'
            },
            {
                'Name': 'imo',
                'Value': str(message['imo']),
                'DimensionValueType': 'VARCHAR'
            },
            {
                'Name': 'ship_type',
                'Value': str(message['ship_type']),
                'DimensionValueType': 'VARCHAR'
            },
        ]
        if message['shipname'] != "":
            dimensions.insert(2, {
                'Name': 'name',
                'Value': str(message['shipname']),
                'DimensionValueType': 'VARCHAR'
            })
        if message['callsign'] != "":
            dimensions.insert(3, {
                'Name': 'callsign',
                'Value': str(message['callsign']),
                'DimensionValueType': 'VARCHAR'
            })
        return {
            'Dimensions': dimensions,
            'MeasureName': 'status',
            'MeasureValueType': 'MULTI'
        }
    return {}


def get_measures(message): 
    """
    Returns measure by msg_type
    """
    if message['msg_type'] in [1,2,3]:
        return [
            {
                'Name': 'longitude',
                'Value': str(message['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(message['lat']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'speed',
                'Value': str(message['speed']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'course',
                'Value': str(message['course']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'turn',
                'Value': str(message['turn']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'status',
                'Value': str(message['status']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'maneuver',
                'Value': str(message['maneuver']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'heading',
                'Value': str(message['heading']),
                'Type': 'BIGINT'
            },
        ]
    if message['msg_type'] == 4:
        return [
            {
                'Name': 'longitude',
                'Value': str(message['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(message['lat']),
                'Type': 'DOUBLE'
            },
        ]
    if message['msg_type'] == 5:
        measures = [
            {
                'Name': 'to_bow',
                'Value': str(message['to_bow']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'to_stern',
                'Value': str(message['to_stern']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'to_port',
                'Value': str(message['to_port']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'to_starboard',
                'Value': str(message['to_starboard']),
                'Type': 'BIGINT'
            },
            {
                'Name': 'draught',
                'Value': str(message['draught']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'eta',
                'Value': get_eta(message),
                'Type': 'TIMESTAMP'
            },
        ]
        if message['destination'] != "":
            measures.insert(0, {
                'Name': 'destination',
                'Value': str(message['destination']),
                'Type': 'VARCHAR'
            })
        return measures
    if message['msg_type'] in [18,19]:
        return [
            {
                'Name': 'longitude',
                'Value': str(message['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(message['lat']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'speed',
                'Value': str(message['speed']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'course',
                'Value': str(message['course']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'heading',
                'Value': str(message['heading']),
                'Type': 'BIGINT'
            },
        ]
    if message['msg_type'] == 27:
        return [
            {
                'Name': 'longitude',
                'Value': str(message['lon']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'latitude',
                'Value': str(message['lat']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'speed',
                'Value': str(message['speed']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'course',
                'Value': str(message['course']),
                'Type': 'DOUBLE'
            },
            {
                'Name': 'status',
                'Value': str(message['status']),
                'Type': 'BIGINT'
            },
        ]
    return []


def get_timestream_table(msg_type):
    """
    Get which table you are writing to from the msg_type
    """
    if msg_type in [1,2,3,4,18,19,27]:
        return "Positions"
    if msg_type == 5:
        return "Status"
    return ""
    

def write_data_to_timestream(message):
    """
    Inserts message to timestream
    """
    logging.debug(message)
    now = round(time.time())
    records = [{
        'Time': str(now),
        'TimeUnit': 'SECONDS',
        'Version': now,
        'MeasureValues': get_measures(message)
    }]
    try:
        logging.debug(f"Writing to {get_timestream_table(message['msg_type'])}")
        logging.debug(records)
        return write_client.write_records(DatabaseName='AIS', TableName=get_timestream_table(message['msg_type']), Records=records, CommonAttributes=get_attributes(message))
    except write_client.exceptions.RejectedRecordsException as error:
        logging.error(f"RejectedRecords: {error}")
        for rejected_record in error.response["RejectedRecords"]:
            logging.error(f"Rejected Index {str(rejected_record['RecordIndex'])}:{rejected_record['Reason']}")
            if 'ExistingVersion' in rejected_record:
                logging.error(f"Existing Version so ignored message: {message}")
    except Exception as error:
        logging.info(f" Error occurred on {message}")
        logging.error(error)


def prep_message_for_timestream(message):
    """
    Filters & preps for Timestream.
    """
    try:
        message['status'] = message['status'].decode("utf-8")
        message['status'] = message['status'].split('.')[1].split(':')[0]
    except:
        pass
    try:
        message['maneuver'] = message['maneuver'].decode("utf-8")
        message['maneuver'] = message['maneuver'].split('.')[1].split(':')[0]
    except:
        pass
    try:
        if (message['turn']) == -0.0:
            message['turn'] = 0.0
    except:
        pass
    try:
        timestream_response = write_data_to_timestream(message)
    except Exception as error:
        logging.error(f"Kinesis - Error Msg: {error} - Processing: {str(message)}")
        raise Exception
    else:
        return timestream_response


def filter_messages(message):
    """
    Checks decoded message is of a type we care about & sends it to process it.
    """
    if message is not None and str(message['msg_type']) in supported_msg_types:
        try:
            prep_message_for_timestream(message)
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
            filter_messages(decoded_message)
        except Exception as error:
            logging.error(f"Error occured decoding: {message}")
            logging.error(error)  

           
def decode_multipart_message(parts):
    """
    Decode multi-part AIS Messages
    """
    try:
        multipart_message = []
        for part in parts:
            prepped_part = prep_message_for_decoding(part).replace("\r\n","")
            if "AIVDM" in prepped_part:
                multipart_message.append(prepped_part)
        logging.debug(f"Raw Multipart - {multipart_message}")
        decodedAISMessage = decode(*multipart_message).asdict()
        logging.debug(f"AIS Decoded Multipart - {decodedAISMessage}")
        filter_messages(decodedAISMessage)
    except Exception as error:
        logging.error(f"Error occured decoding: {parts}")
        logging.error(error)


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
    logging.info('Orbcomm Ingester Script Starting ...')
    # Load the CA certificates used for validating the peer's certificate.
    ssl_context.load_verify_locations(cafile=os.path.relpath(certifi.where()),capath=None, cadata=None)
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
    server_certificate = secure_client_socket.getpeercert()
    logging.debug("Certificate obtained from the server:")
    logging.debug(server_certificate)
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
        Sends relevant data to Timestream Database.
    """
    while True:
        logging.info('Orbcomm Ingester Script Starting ...')
        socket = get_orbcomm_socket() 
        try:
            first_part_of_multipart_message = ""
            multipart_message = []
            start_time = datetime.today();
            message_counter = 0
            empty_message_counter = 0
            time_of_first_encountered_empty_message = start_time;
            time_to_throw_an_exception = start_time;
            process_pool = Pool()
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
                            time_to_throw_an_exception = time_of_first_encountered_empty_message + datetime.timedelta(seconds=5)
                        if empty_message_counter > 1 and time_to_throw_an_exception < datetime.today():
                            raise Exception(f"Something interrupted the stream since: {time_of_first_encountered_empty_message}")
                        continue
                    
                    empty_message_counter = 0
                    message_counter +=1
                    if message_counter % 1000 == 0:
                        now = datetime.today()
                        interval = now - start_time
                        logging.info(f"1000 messages processed in {interval.total_seconds()} seconds")
                        start_time = now

                    logging.debug(f"Raw - {raw_message}")
                    logging.debug(f"Decoded utf-8 - {encoded_message}")
                    
                    # Check that we are dealing with the second part of a multipart msg.
                    # N.B. This solution only deals with 2 part msgs.
                    if first_part_of_multipart_message != "" and 'AIVDM,2,2' in encoded_message:
                        logging.debug(f"First part  - {first_part_of_multipart_message}")
                        logging.debug(f"Second part - {encoded_message}")
                        multipart_message = [first_part_of_multipart_message, encoded_message]
                        if multiprocessing_is_enabled:
                            try:
                                result = process_pool.apply_async(decode_multipart_message, [multipart_message])
                                logging.debug(result.get(timeout=1))
                            except Exception as error:
                                logging.error(error)
                        else:
                            decode_multipart_message(multipart_message)
                        first_part_of_multipart_message = ""
                        continue
                    
                    # Check for multipart msgs
                    # N.B. This solution only deals with 2 part msgs.
                    if 'AIVDM,2,1' in encoded_message:
                        first_part_of_multipart_message = encoded_message
                        continue
                    
                    if multiprocessing_is_enabled:
                        try:
                            result = process_pool.apply_async(decode_single_part_message, [encoded_message])
                            logging.debug(result.get(timeout=1))
                        except Exception as error:
                            logging.error(error)
                    else: 
                        decode_single_part_message(encoded_message)
                    continue
                except Exception as error:
                    logging.error(error)
                    raise Exception
        except Exception as error:
            logging.error(error)
            raise Exception
