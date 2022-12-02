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
# Context creation
sslContext              = ssl.SSLContext()
sslContext.verify_mode  = ssl.CERT_REQUIRED
# Check for OS X platform
import certifi
import os

debugging = os.environ.get("DEBUGGING", False)
supportedAISmsgTypes = ['1','2','3','4','5','18']

def debug(msg):
    if(debugging): 
        print(msg)

def put_kinesis(msg_body):
    """
    Sends a message to Kinesis.
    """
    import json
    import boto3
    
    AWS_REGION = 'eu-west-2'
    kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
    hashkey = str(msg_body['msg_type'])
    stream_name = "ais_msg_type_{}".format(str(msg_body['msg_type']))
    put_response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(msg_body),
        PartitionKey=hashkey)
    return put_response


def stream_message(msg_body):
    """
    Sends a message to the specified queue.
    """
    if str(msg_body['msg_type']) not in supportedAISmsgTypes:
        return False
    debug(msg_body['msg_type'])
    # TODO Refactor
    try:
        put_response = put_kinesis(msg_body)
    except Exception as err:
        try:
            msg_body['data'] = str(msg_body['data'].decode())
        except:
            print('no data to decode')
        try:
            msg_body['spare_1'] = str(msg_body['spare_1'].decode())
        except:
            print('no spare_1 to decode')
        try:
            msg_body['status'] = str(msg_body['status'].decode())
        except:
            print('no data to decode')
        try:
            msg_body['spare_1'] = str(msg_body['spare_1'].decode("utf-8"))
            put_response = put_kinesis(msg_body)
        except:
            try:
                try:
                    del(msg_body['status'])
                except:
                    print('no status to del')
                try:
                    del(msg_body['data'])
                except:
                    print('no data to del')
                try:
                    del(msg_body['spare_1'])
                except:
                    print('no spare_1 to del')
                put_response = put_kinesis(msg_body)
            except:
                try:
                    try:
                        last_key = list(msg_body.keys())[-1]
                        msg_body[last_key] = str(msg_body[last_key].decode("utf-8"))
                    except:
                        print('try to decode last item')        
                    put_response = put_kinesis(msg_body)
                except Exception as err:
                    print(str(msg_body))
                    print(err)
            else:
                return put_response
        else:
            return put_response
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


first_part = ""
parts = []
decoded_message = None
def is_2_part(signal):
    if 'g:1-2-' in signal:
        return True


def create_multi_part(parts):
    decoded_parts = []
    for part in parts:
        data = part.split("!")
        string_to_decode = '!' + str(data[1])
        string_to_decode = string_to_decode.replace("\r\n","")
        if "AIVDM" in string_to_decode:
            decoded_parts.append(string_to_decode)
    return decoded_parts


while True:
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # Load the CA certificates used for validating the peer's certificate
    sslContext.load_verify_locations(cafile=os.path.relpath(certifi.where()),capath=None, cadata=None)
    # Create an SSLSocket                                   
    clientSocket = socket.create_connection(("globalais2.orbcomm.net", 9054))
    secureClientSocket = sslContext.wrap_socket(clientSocket, do_handshake_on_connect=False)
    # Only connect, no handshake
    t1 = time.time()
    print("Time taken to establish the connection:%2.3f"%(time.time() - t1))
    # Explicit handshake
    t3 = time.time()
    secureClientSocket.do_handshake()
    print("Time taken for SSL handshake:%2.3f"%(time.time() - t3))
    # Get the certificate of the server and print
    serverCertificate = secureClientSocket.getpeercert()
    print("Certificate obtained from the server:")
    print(serverCertificate) 
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
        print(e)
        continue
    try:
        while True:
            try:
                row = secureClientSocket.recv(4096)
                debug(f"1. raw {row}")
                if row !="":
                    row_to_string = row.decode("utf-8") 
                    debug(f"2. string {row_to_string}")
                    if is_2_part(row_to_string):
                        first_part = row_to_string
                        debug(first_part)
                    else:
                        if first_part == "":
                            data = row_to_string.split("!")
                            string_to_decode = '!' + str(data[1])
                            if string_to_decode.find("AIVDM") != -1:
                                decoded_message = decode(string_to_decode).asdict()
                                debug(f"decoded_message single {decoded_message}")
                        if first_part !="" and 'g:2-2-' in row_to_string:
                            debug(f"first part {first_part}")
                            debug(f"second part {row_to_string}")
                            parts = [first_part, row_to_string]
                            multipart = create_multi_part(parts)
                            debug(f"multipart {multipart}")
                            decoded_message = decode(*multipart).asdict()
                            debug(f"decoded_message multi {decoded_message}")
                            first_part = ""
                            debug(f" reset first part{first_part}")
                            debug('decoded msg')
                            debug(f"3. {decoded_message}\n")
                        if decoded_message is not None:
                            try:
                                stream_response = stream_message(decoded_message)
                                debug(stream_response)
                            except Exception as error:
                                print(error)
            except Exception as e:
                if data[0] == "":
                    print(e)
    except Exception as error:
        print(error)
