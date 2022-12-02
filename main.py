from shutil import ExecError
import socket
import ssl
import platform
from datetime import datetime
import time
from pyais import decode
import json
import sys, traceback
# Context creation
sslContext              = ssl.SSLContext()
sslContext.verify_mode  = ssl.CERT_REQUIRED
# Check for OS X platform
import certifi
import os
import boto3
from botocore.exceptions import ClientError

def put_kinesis(msg_body):
    import json
    import boto3
    from models import Orbcomm_Vessels
    from django.http import Http404
    from django.shortcuts import get_object_or_404
    
    AWS_REGION = 'eu-west-2'
    kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
    hashkey = str(msg_body['msg_type'])
    try:
        vessel = get_object_or_404(Orbcomm_Vessels, mmsi=msg_body["mmsi"]) 
    except Http404:
        put_response = kinesis_client.put_record(
                StreamName="SundryAIS",
                Data=json.dumps(msg_body),
                PartitionKey=hashkey)
        return put_response
    if vessel.is_ais_enabled == 1:
        stream_name = "ais_msg_type_{}".format(str(msg_body['msg_type']))
        put_response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(msg_body),
                PartitionKey=hashkey)
        return put_response
    else:
        put_response = kinesis_client.put_record(
                StreamName="HistoricalAIS",
                Data=json.dumps(msg_body),
                PartitionKey=hashkey)
        return put_response


def stream_message(msg_body):
    """
    Sends a message to the specified queue.
    """
    if str(msg_body['msg_type']) not in ['1','2','3','4','5','18']:
        return False

    print(msg_body['msg_type'])
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
                    print("line number 37")
                    print(str(msg_body))
                    print(err)
            else:
                return put_response
        else:
            return put_response
    else:
        return put_response


def response_object(msg_object):
    import pytz
    import datetime as dt
    from dateutil.relativedelta import relativedelta
    from models import Orbcomm_Vessels, Orbcomm_Positions
    from django.utils import timezone
    response = {}


    if msg_object['msg_type'] == 1 or msg_object['msg_type'] == 2 or msg_object['msg_type'] == 3 or msg_object['msg_type'] == 21 or msg_object['msg_type'] == 19:
        try:
            print (f"69 whole msg 1, 2, 3, 19, 21 object {msg_object}")
            vessel, created = Orbcomm_Vessels.objects.get_or_create(mmsi=msg_object["mmsi"])
            if vessel.vessel_type:
                if vessel.vessel_type in ['AntiPollutionEquipment', 'DivingOps', 'DredgingOrUnderwaterOps', 'Fishing', 'HSC', 'HSC_HazardousCategory_A', 'HSC_HazardousCategory_B', 'HSC_HazardousCategory_C', \
                    'HSC_HazardousCategory_D', 'HSC_NoAdditionalInformation', 'HSC_Reserved', 'LawEnforcement', 'MedicalTransport', 'MilitaryOps', 'NonCombatShip',  \
                    'Passenger', 'Passenger_HazardousCategory_A', 'Passenger_HazardousCategory_C', 'Passenger_HazardousCategory_D', 'Passenger_NoAdditionalInformation', \
                    'Passenger_Reserved', 'PleasureCraft', 'Reserved', 'Sailing', 'SearchAndRescueVessel', 'SPARE', 'WIG', 'WIG_HazardousCategory_B', 'WIG_HazardousCategory_C', 'WIG_Reserved']:
                    return
            position = Orbcomm_Positions()
            position.lon = msg_object.get('lon')
            position.lat = msg_object.get('lat')
            position.course = msg_object.get('course')
            position.heading = msg_object.get('heading')

            position.status = str(msg_object.get('status')).replace('NavigationStatus.', '')

            position.accuracy = msg_object.get('accuracy')
    
            position.speed =msg_object.get('speed')

            vessel.position_updated_at = timezone.now()
            position.mmsi = vessel
            try:
                print(f"92 type 1, 2, 3, 19, 21 POSITION {position}")
                print(f"93 type 1, 2, 3, 19, 21 VESSEL {vessel}")
                position.save()
                vessel.save()
            except Exception as e:
                print(f"97 ERROR: {e}")
                raise Exception
            print(f"98 transaction committed {vessel}\n\n")
        except Exception:
            full_traceback = traceback.format_exc()
            print(full_traceback)
            raise Exception
 
    
    # elif msg_object['msg_type'] == 18:
    #     try:
    #         print (f"104 whole msg 18 object {msg_object}")
    #         vessel, created = Orbcomm_Vessels.objects.get_or_create(mmsi=msg_object["mmsi"])

    #         if vessel.vessel_type:
    #             if vessel.vessel_type in ['AntiPollutionEquipment', 'DivingOps', 'DredgingOrUnderwaterOps', 'Fishing', 'HSC', 'HSC_HazardousCategory_A', 'HSC_HazardousCategory_B', 'HSC_HazardousCategory_C', \
    #                 'HSC_HazardousCategory_D', 'HSC_NoAdditionalInformation', 'HSC_Reserved', 'LawEnforcement', 'MedicalTransport', 'MilitaryOps', 'NonCombatShip',  \
    #                 'Passenger', 'Passenger_HazardousCategory_A', 'Passenger_HazardousCategory_C', 'Passenger_HazardousCategory_D', 'Passenger_NoAdditionalInformation', \
    #                 'Passenger_Reserved', 'PleasureCraft', 'Reserved', 'Sailing', 'SearchAndRescueVessel', 'SPARE', 'WIG', 'WIG_HazardousCategory_B', 'WIG_HazardousCategory_C', 'WIG_Reserved']:
    #                 return
    #         position = Orbcomm_Positions()
    #         position.lon = msg_object.get('lon')
    #         position.lat = msg_object.get('lat')
    #         position.course = msg_object.get('course')
    #         position.heading = msg_object.get('heading')
            
    #         vessel.position_updated_at = timezone.now()
    #         position.mmsi = vessel
    #         try:
    #             print(f"124 type 18 POSITION {position}")
    #             print(f"125 type 18 VESSEL {vessel}\n\n")
    #             position.save()
    #             vessel.save()
    #         except Exception as e:
    #             print(f"129 ERROR: {e}")
    #             raise Exception
    #         print(f"130 transaction committed {vessel}")
    #     except Exception:
    #         full_traceback = traceback.format_exc()
    #         print(full_traceback)
    #         raise Exception
 

                    
    elif msg_object['msg_type'] == 5:
        
        try:
            print (f"140 whole msg 5 object {msg_object}")
            try:
                if msg_object['month']== 0 or msg_object['day']==0 or msg_object['hour'] > 23 or msg_object['day'] > 31 or msg_object['minute'] > 60:
                    eta_date = timezone.now() + relativedelta(months=msg_object['month'], days=msg_object['day'],hours=msg_object['hour'], minutes=msg_object['minute'] )
                else:    
                    eta_date = dt.datetime(dt.date.today().year, msg_object['month'], msg_object['day'], msg_object['hour'], msg_object['minute'], tzinfo=pytz.UTC)
                    if eta_date < timezone.now() - relativedelta(months=1):
                        eta_date = eta_date + relativedelta(years=+1)
            except Exception as e:
                eta_date = None
                print (f" 150 {e}")
                raise Exception
            try:
                vessel, created = Orbcomm_Vessels.objects.update_or_create( mmsi=msg_object['mmsi'], defaults= { \
                'mmsi': msg_object['mmsi'], \
                'imo': msg_object['imo'],  \
                'vessel_name': msg_object['shipname'], \
                'vessel_callsign': msg_object['callsign'], \
                'vessel_type': str(msg_object['ship_type']), \
                'destination': msg_object['destination'], \
                'eta': eta_date, \
                'draught': msg_object['draught'], \
                'to_bow': msg_object['to_bow'], \
                'to_starboard': msg_object['to_starboard'], \
                'to_stern': msg_object['to_stern'], \
                'to_port': msg_object['to_port']
                })
                
                print(f"165 transaction committed {vessel}")
                print(f"167 type 5 VESSEL {vessel}\n\n")
                vessel.save()

            except Exception as e:
                eta_date = None
                print(f" Exception 173 {e}")
                raise Exception
        except Exception:
            full_traceback = traceback.format_exc()
            print(full_traceback)
            raise Exception
            
    else:
        print(f"176 THE LAST RESPONSE! {response}\n\n")
        print(f"177 message is = {msg_object}")
        pass
    return response

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
    nmea_string = "$PMWLSS,{},5,{},{},1*".format(time,user_name,password)
    chksum_string = nmea_string[1:]
    package = "{}{}\r\n".format(nmea_string, checksum(chksum_string))
    b = bytearray()
    b.extend(package.encode('utf-8'))
 
    secureClientSocket.send(b)
    try:
        while True:
            try:
                row = secureClientSocket.recv(4096)
                print(f"1. raw {row}")
                if row !="":
                    row_to_string = row.decode("utf-8") 
                    print(f"2. string {row_to_string}")
                    if is_2_part(row_to_string):
                        first_part = row_to_string
                        print (first_part)
                        pass
                    if first_part == "":
                        data = row_to_string.split("!")
                        string_to_decode = '!' + str(data[1])
                        if string_to_decode.find("AIVDM") != -1:
                            decoded_message = decode(string_to_decode).asdict()
                            print (f"decoded_message single {decoded_message}")
                    if first_part !="" and 'g:2-2-' in row_to_string:
                        print(f"first part {first_part}")
                        print(f"second part {row_to_string}")
                        parts = [first_part, row_to_string]
                        multipart = create_multi_part(parts)
                        print(f"multipart {multipart}")
                        decoded_message = decode(*multipart).asdict()
                        print (f"decoded_message multi {decoded_message}")
                        first_part = ""
                        print(f" reset first part{first_part}")
                    print (f"3. {decoded_message}\n")
                    if decoded_message is not None:
                        try:
                            print("RIGHT BEFORE RESPONSE OBJECT")
                            response_object(decoded_message)
                            try:  
                                stream_response = stream_message(decoded_message)
                                print(stream_response)
                            except Exception as error:
                                print("line number 281")
                                print(error)
                                raise Exception
                        except Exception as error:
                            print("line number 285")
                            print(error)
                            raise Exception
            except IndexError as e:
                if data[0] == "":
                    raise

    except Exception as error:
        print("line number 189")
        print(error)