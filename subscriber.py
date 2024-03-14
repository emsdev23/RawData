import paho.mqtt.client as mqtt
from datetime import datetime
import tzlocal
import mysql.connector
import ast
import time
import logging 
import decimal
import json
import Class_Definition
#creating and configure logger
logging.basicConfig(filename="logs.log",format='%(asctime)s %(message)s',filemode ='w')

#creating an object
logger = logging.getLogger()

#setting the level
logger.setLevel(logging.DEBUG)

try:
    conn = None
    conn = mysql.connector.connect(host='localhost',database='EMS',user='root',password='22@teneT')
    cursor = conn.cursor()
except Exception as error:
     logger.error(error)

# Initialize the start and end time variables
charging_start_time = None
charging_end_time = None

discharging_start_time = None
discharging_end_time = None

# Flag to check if start time and end time has been assigned
is_charg_start_time_assigned = False
is_discharg_start_time_assigned = False

is_charg_end_time_assigned = False
is_discharg_end_time_assigned = False
    

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
     
  client.subscribe(Class_Definition.Config.config['Topics']['Load'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['Battery'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['Charger'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['CV'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['CT'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['Specs'],1)


def on_message(client, userdata, message):
    data = message.payload
    dict_str = data.decode("UTF-8")  #converting byte to dict
    data1 = ast.literal_eval(dict_str) #converting byte to dic
    print('BPS',data1)

    
    def charging(status,dischg_start_time):
         
         global is_charg_start_time_assigned,is_discharg_start_time_assigned,is_charg_end_time_assigned,is_discharg_end_time_assigned
         global charging_start_time ,charging_end_time ,discharging_start_time ,discharging_end_time
         
         chg_endtime = []
         is_discharg_end_time_assigned = False         
         if(data1["BPS"] == 1) :
             
             chg_endtime.append(data1['BRTC'])
             if not is_charg_start_time_assigned :
                 charging_start_time = data1['BRTC']
                 
                 discharging(data1["BPS"],charging_start_time)  #charging start time is discharging end time
                 
                 is_charg_start_time_assigned = True
                 
                 start_timestamp_unix = charging_start_time   #unix to local timestamp
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 start_timestamp_local = datetime.fromtimestamp(start_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
                 
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upschargingstarttime) VALUE (%s)"""
                 record_to_insert = (start_timestamp_local)
                 cursor.execute(insert_query,(record_to_insert,))
        
                 conn.commit()
                 
             
         elif (data1["BPS"] == 2 or data1["BPS"] == 0) :
             if not is_charg_end_time_assigned :
                 #charging_end_time = chg_endtime.pop()
                 charging_end_time = dischg_start_time
                 is_charg_end_time_assigned = True
                 
                 is_charg_start_time_assigned = False
                 
                 chg_endtime = []
                 
         
                 end_timestamp_unix = charging_end_time
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 end_timestamp_local = datetime.fromtimestamp(end_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime  
         
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upschargingendtime) VALUE (%s)"""
                 record_to_insert = (end_timestamp_local)
                 cursor.execute(insert_query, (record_to_insert,))
        
                 conn.commit()
         
                      
                 
    def discharging(status,charg_start_time) :
    
         global is_charg_start_time_assigned,is_discharg_start_time_assigned,is_charg_end_time_assigned,is_discharg_end_time_assigned
         global charging_end_time ,discharging_start_time ,discharging_end_time
         #global charging_start_time
             
         dchg_endtime = [] 
         is_charg_end_time_assigned = False
         if(data1["BPS"] == 2) :
             
             dchg_endtime.append(data1['BRTC'])
             if not is_discharg_start_time_assigned :
                 discharging_start_time = data1['BRTC']
                 
                 charging(data1["BPS"],discharging_start_time)   #discharging start time is charging end time
                 
                 is_discharg_start_time_assigned = True
                 
                 start_timestamp_unix = discharging_start_time    #unix to localtime
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 start_timestamp_local = datetime.fromtimestamp(start_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
                 #print(start_timestamp_local,121)
                 
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upsdischargingstartingtime) VALUES (%s)"""
                 record_to_insert = (start_timestamp_local)
                 cursor.execute(insert_query,(record_to_insert,))
        
                 conn.commit()
                 
             
         elif (data1["BPS"] == 1 or data1["BPS"] == 0) :
             if not is_discharg_end_time_assigned :
                 #discharging_end_time = dchg_endtime.pop()
                 discharging_end_time= charg_start_time
                 is_discharg_end_time_assigned = True
                 
                 is_discharg_start_time_assigned = False
                 
                 dchg_endtime = []
                 
                 end_timestamp_unix = discharging_end_time
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 end_timestamp_local = datetime.fromtimestamp(end_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime  
                 #print(end_timestamp_local,143)
   
         
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upsdischargingendtime) VALUES (%s)"""
                 record_to_insert = (end_timestamp_local)
                 cursor.execute(insert_query, (record_to_insert,))
        
                 conn.commit()
    
    if message.topic == Class_Definition.Config.config['Topics']['Load']:
        print(data1)
        totalpower = data1["WTT"]
        powerrphase = data1["WTR"]
        poweryphase = data1["WTY"]
        powerbphase = data1["WTB"]
        powerfactoravarage= data1["PFA"]
        powerfactorrphase =data1["PFR"]
        powerfactoryphase=data1["PFY"]
        powerfactorbphase = data1["PFB"]
        totalapperantpower = data1["VAT"]
        apparantpowerrphase = data1["VAR"]
        apparantpoweryphase =data1["VAY"]
        apparantpowerbphase = data1["VAB"]
        voltagelinetolineavarage = data1["VLL"]
        voltageryphase = data1["VRY"]
        voltageybphase = data1["VYB"]
        voltagebrphase =data1["VBR"]
        voltagelineneutralavaerage = data1["VLN"]
        voltagerphase= data1["VR"]
        voltageyphase=data1["VY"]
        voltagebphase = data1["VB"]
        currenttotal = data1["CT"]
        currentrphase =data1["CR"]
        currentyphase = data1["CY"]
        currentbphase = data1["CB"]
        frequency = data1["FZ"]
        activeenergyeb =data1["WhEB"]
        apparantenergyeb = data1["VhEB"]
        activeenergydg = data1["WHDG"]
        apparantenergydg = data1["VhDG"]
        loadhourseb = data1["LHEB"]
        loadhoursdb = data1["LHDG"]
        timestamp_unix = data1['LRTC']


            #converting from unix timestamp to local time
        local_timezone = tzlocal.get_localzone() # get pytz timezone
        timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
        #print(timestamp_local,178) 
         
        #inserting into database
        insert_query = """ INSERT INTO EMSUPSLoad (loadtimestamp,totalpower,power_R_phase,power_Y_phase,power_B_phase,powerfactoravarage,powerfactor_R_phase,powerfactor_Y_phase,powerfactor_B_phase,totalapparantpower,apparantpower_R_phase,apparantpower_Y_phase,apparantpower_B_phase,voltagelinetolineaverage,voltage_RY_phase,voltage_YB_phase,voltage_BR_phase,voltagelinenuetralaverage,voltage_R_phase,voltage_Y_phase,voltage_B_phase,currenttotal,current_R_phase,current_Y_phase,current_B_phase,frequency,activeenergy_EB,apparantenergy_EB,activeenergy_DG,apparantenergy_DG,loadhours_EB,loadhours_DG) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (timestamp_local,totalpower,powerrphase,poweryphase,powerbphase,powerfactoravarage,powerfactorrphase,powerfactoryphase,powerfactorbphase,totalapperantpower,apparantpowerrphase,apparantpoweryphase,apparantpowerbphase,voltagelinetolineavarage,voltageryphase,voltageybphase,voltagebrphase,voltagelineneutralavaerage,voltagerphase,voltageyphase,voltagebphase,currenttotal,currentrphase,currentyphase,currentbphase,frequency,activeenergyeb,apparantenergyeb,activeenergydg,apparantenergydg,loadhourseb,loadhoursdb)
        cursor.execute(insert_query, record_to_insert)
        print(record_to_insert)
        print("UPS LOAD Inserted")
        conn.commit()




    
    if message.topic == Class_Definition.Config.config['Topics']['Battery'] :
         print('BPS',data1)
         
             
           
         if(data1["BPS"] == "001.00") :
             battery_status =  "CHG"
             charging(data1["BPS"],data1['BRTC'])  #if status is 1  then goto charging funtion
             
         elif (data1["BPS"] == "002.00") :
             battery_status =  "DCHG"  
             discharging(data1["BPS"],data1['BRTC'])  #if status is 2 then goto discharging funtion
             
         elif(data1["BPS"] == "000.00") :
             battery_status =  "IDLE"

         else:
            battery_status = " invalid"
         #battery_status = data1["BPS"]    
         charge_energy = data1.get("BCHE", "")  # Get the value of "BCHE" key from data1 dictionary
         charge_energy = decimal.Decimal(charge_energy) if charge_energy else decimal.Decimal(0)
         discharge_energy = data1.get("BDCHE", "")  # Get the value of "BDCHE" key from data1 dictionary
         discharge_energy = decimal.Decimal(discharge_energy) if discharge_energy else decimal.Decimal(0)  # Convert to decimal format and handle empty string
         print('dchg Energy',discharge_energy)  # Output the value of discharge_energy in decimal format

         pack_usable_soc = data1["BUSOC"]
         timestamp_unix = data1['BRTC']
         batteryvoltage = data1["BV"]
         batterycurrent = data1["BC"]
         contactorstatues = data1["BCS"]
         precontacrorstaues = data1['BPCS']
         packsoc = data1["BSOC"]
         negative_energy = data1.get("BDCHE", "")  # Get the value of "BDCHE" key from data1 dictionary
         if negative_energy:
             negative_energy = decimal.Decimal(negative_energy)  # Convert to decimal format
             if negative_energy > 0:  # Check if the value is positive
                 negative_energy = negative_energy * decimal.Decimal(-1)
                 print(negative_energy)  # Make the value negative
         else:
             negative_energy = decimal.Decimal(0)  # Handle empty string
             print(negative_energy)

         timestamp_unix = int(data1['BRTC'])
         #converting from unix timestamp to local time
         local_timezone = tzlocal.get_localzone() # get pytz timezone
         timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
         #print(timestamp_local,178)
         current_time = datetime.now()


            
         #inserting into database
         insert_query = """ INSERT INTO EMSUPSBattery (upstimestamp,upschargingenergy,upsdischargingenergy,pack_usable_soc,upsbatterystatus,batteryvoltage,batterycurrent,contactorstatus,precontactorstatus,packsoc,received_time,negative_energy) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (timestamp_local,charge_energy,discharge_energy,pack_usable_soc,battery_status,batteryvoltage,batterycurrent,contactorstatues,precontacrorstaues,packsoc,current_time,negative_energy)
         print(record_to_insert)
         print("UPS Battery data Inserted")
         cursor.execute(insert_query, record_to_insert)
         conn.commit()


    if message.topic == Class_Definition.Config.config['Topics']['Charger'] :
         print(data1)
         chargevoltage1 = data1["CV1"]
         chargevoltage2 = data1["CV2"]
         chargecurrent1 = data1["CC1"]
         chargecurrent2 = data1["CC2"]

         timestamp_unix = data1['CRTC']
         timestamp_unix = int(timestamp_unix)
          # convert timestamp_unix from string to integer
         local_timezone = tzlocal.get_localzone() # get pytz timezone
         timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime

    
         
         #inserting into database
         insert_query = """ INSERT INTO EMSUPSCharger(chargetimestamp,chargevoltage1,chargevoltage2,chargecurrent1,chargecurrent2) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (timestamp_local,chargevoltage1,chargevoltage2,chargecurrent1,chargecurrent2)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
    if message.topic == Class_Definition.Config.config['Topics']['CV'] :
        print(data1)
        module_1_cv1_value = int(data1['Module_1'][0]['CV1'])
        print(f"Module_1 CV1 value: {module_1_cv1_value}")

        module_2_cv1_value = int(data1['Module_2'][0]['CV1'])
        print(f"Module_2 CV1 value: {module_2_cv1_value}")

        module_3_cv1_value = int(data1['Module_3'][0]['CV1'])
        print(f"Module_3 CV1 value: {module_3_cv1_value}")

        module_4_cv1_value = int(data1['Module_4'][0]['CV1'])
        print(f"Module_4 CV1 value: {module_4_cv1_value}")

        module_5_cv1_value = int(data1['Module_5'][0]['CV1'])
        print(f"Module_5 CV1 value: {module_5_cv1_value}")

        module_6_cv1_value = int(data1['Module_6'][0]['CV1'])
        print(f"Module_6 CV1 value: {module_6_cv1_value}")
        
        module_7_cv1_value = int(data1['Module_7'][0]['CV1'])
        print(f"Module_7 CV1 value: {module_7_cv1_value}")

        module_8_cv1_value = int(data1['Module_8'][0]['CV1'])
        print(f"Module_8 CV1 value: {module_8_cv1_value}")

        module_9_cv1_value = int(data1['Module_9'][0]['CV1'])
        print(f"Module_9 CV1 value: {module_9_cv1_value}")

        module_10_cv1_value = int(data1['Module_10'][0]['CV1'])
        print(f"Module_10 CV1 value: {module_10_cv1_value}")

        module_11_cv1_value = int(data1['Module_11'][0]['CV1'])
        print(f"Module_11 CV1 value: {module_11_cv1_value}")

        module_12_cv1_value = int(data1['Module_12'][0]['CV1'])
        print(f"Module_12 CV1 value: {module_12_cv1_value}")
        module_13_cv1_value = int(data1['Module_13'][0]['CV1'])
        print(f"Module_13 CV1 value: {module_13_cv1_value}")

        module_14_cv1_value = int(data1['Module_14'][0]['CV1'])
        print(f"Module_14 CV1 value: {module_14_cv1_value}")

        module_15_cv1_value = int(data1['Module_15'][0]['CV1'])
        print(f"Module_15 CV1 value: {module_15_cv1_value}")

        module_16_cv1_value = int(data1['Module_16'][0]['CV1'])
        print(f"Module_16 CV1 value: {module_16_cv1_value}")
        module_1_cv2_value = int(data1['Module_1'][0]['CV2'])
        print(f"Module_1 CV2 value: {module_1_cv2_value}")

        module_2_cv2_value = int(data1['Module_2'][0]['CV2'])
        print(f"Module_2 CV2 value: {module_2_cv2_value}")

        module_3_cv2_value = int(data1['Module_3'][0]['CV2'])
        print(f"Module_3 CV2 value: {module_3_cv2_value}")

        module_4_cv2_value = int(data1['Module_4'][0]['CV2'])
        print(f"Module_4 CV2 value: {module_4_cv2_value}")

        module_5_cv2_value = int(data1['Module_5'][0]['CV2'])
        print(f"Module_5 CV2 value: {module_5_cv2_value}")

        module_6_cv2_value = int(data1['Module_6'][0]['CV2'])
        print(f"Module_6 CV2 value: {module_6_cv2_value}")
        
        module_7_cv2_value = int(data1['Module_7'][0]['CV2'])
        print(f"Module_7 CV2 value: {module_7_cv2_value}")

        module_8_cv2_value = int(data1['Module_8'][0]['CV1'])
        print(f"Module_8 CV2 value: {module_8_cv2_value}")

        module_9_cv2_value = int(data1['Module_9'][0]['CV1'])
        print(f"Module_9 CV2 value: {module_9_cv2_value}")

        module_10_cv2_value = int(data1['Module_10'][0]['CV1'])
        print(f"Module_10 CV2 value: {module_10_cv2_value}")

        module_11_cv2_value = int(data1['Module_11'][0]['CV1'])
        print(f"Module_11 CV2 value: {module_11_cv2_value}")

        module_12_cv2_value = int(data1['Module_12'][0]['CV1'])
        print(f"Module_12 CV2 value: {module_12_cv2_value}")
        module_13_cv2_value = int(data1['Module_13'][0]['CV1'])
        print(f"Module_13 CV2 value: {module_13_cv2_value}")

        module_14_cv2_value = int(data1['Module_14'][0]['CV1'])
        print(f"Module_14 CV2 value: {module_14_cv2_value}")

        module_15_cv2_value = int(data1['Module_15'][0]['CV1'])
        print(f"Module_15 CV2 value: {module_15_cv2_value}")

        module_16_cv2_value = int(data1['Module_16'][0]['CV1'])
        print(f"Module_16 CV2 value: {module_16_cv2_value}")
        module_1_cv3_value = int(data1['Module_1'][0]['CV3'])
        print(f"Module_1 CV3 value: {module_1_cv3_value}")

        module_2_cv3_value = int(data1['Module_2'][0]['CV3'])
        print(f"Module_2 CV3 value: {module_2_cv3_value}")

        module_3_cv3_value = int(data1['Module_3'][0]['CV3'])
        print(f"Module_3 CV3 value: {module_3_cv3_value}")

        module_4_cv3_value = int(data1['Module_4'][0]['CV3'])
        print(f"Module_4 CV3 value: {module_4_cv3_value}")

        module_5_cv3_value = int(data1['Module_5'][0]['CV3'])
        print(f"Module_5 CV3 value: {module_5_cv3_value}")

        module_6_cv3_value = int(data1['Module_6'][0]['CV3'])
        print(f"Module_6 CV3 value: {module_6_cv3_value}")
        
        module_7_cv3_value = int(data1['Module_7'][0]['CV3'])
        print(f"Module_7 CV3 value: {module_7_cv3_value}")

        module_8_cv3_value = int(data1['Module_8'][0]['CV3'])
        print(f"Module_8 CV3 value: {module_8_cv3_value}")

        module_9_cv3_value = int(data1['Module_9'][0]['CV3'])
        print(f"Module_9 CV3 value: {module_9_cv3_value}")

        module_10_cv3_value = int(data1['Module_10'][0]['CV3'])
        print(f"Module_10 CV3 value: {module_10_cv3_value}")

        module_11_cv3_value = int(data1['Module_11'][0]['CV3'])
        print(f"Module_11 CV3 value: {module_11_cv3_value}")

        module_12_cv3_value = int(data1['Module_12'][0]['CV3'])
        print(f"Module_12 CV3 value: {module_12_cv3_value}")
        module_13_cv3_value = int(data1['Module_13'][0]['CV3'])
        print(f"Module_13 CV3 value: {module_13_cv3_value}")

        module_14_cv3_value = int(data1['Module_14'][0]['CV3'])
        print(f"Module_14 CV3 value: {module_14_cv3_value}")

        module_15_cv3_value = int(data1['Module_15'][0]['CV3'])
        print(f"Module_15 CV3 value: {module_15_cv3_value}")

        module_16_cv3_value = int(data1['Module_16'][0]['CV3'])
        print(f"Module_16 CV3 value: {module_16_cv3_value}")
        module_1_cv4_value = int(data1['Module_1'][0]['CV4'])
        print(f"Module_1 CV4 value: {module_1_cv4_value}")

        module_2_cv4_value = int(data1['Module_2'][0]['CV4'])
        print(f"Module_2 CV4 value: {module_2_cv4_value}")

        module_3_cv4_value = int(data1['Module_3'][0]['CV4'])
        print(f"Module_3 CV4 value: {module_3_cv4_value}")

        module_4_cv4_value = int(data1['Module_4'][0]['CV4'])
        print(f"Module_4 CV1 value: {module_4_cv4_value}")

        module_5_cv4_value = int(data1['Module_5'][0]['CV4'])
        print(f"Module_5 CV1 value: {module_5_cv4_value}")

        module_6_cv4_value = int(data1['Module_6'][0]['CV4'])
        print(f"Module_6 CV1 value: {module_6_cv4_value}")
        
        module_7_cv4_value = int(data1['Module_7'][0]['CV1'])
        print(f"Module_7 CV1 value: {module_7_cv4_value}")

        module_8_cv4_value = int(data1['Module_8'][0]['CV4'])
        print(f"Module_8 CV4 value: {module_8_cv4_value}")

        module_9_cv4_value = int(data1['Module_9'][0]['CV4'])
        print(f"Module_9 CV4 value: {module_9_cv4_value}")

        module_10_cv4_value = int(data1['Module_10'][0]['CV4'])
        print(f"Module_10 CV4 value: {module_10_cv4_value}")

        module_11_cv4_value = int(data1['Module_11'][0]['CV4'])
        print(f"Module_11 CV4 value: {module_11_cv4_value}")

        module_12_cv4_value = int(data1['Module_12'][0]['CV4'])
        print(f"Module_12 CV4 value: {module_12_cv4_value}")
        module_13_cv4_value = int(data1['Module_13'][0]['CV4'])
        print(f"Module_13 CV4 value: {module_13_cv4_value}")

        module_14_cv4_value = int(data1['Module_14'][0]['CV4'])
        print(f"Module_14 CV4 value: {module_14_cv4_value}")

        module_15_cv4_value = int(data1['Module_15'][0]['CV4'])
        print(f"Module_15 CV4 value: {module_15_cv4_value}")

        module_16_cv4_value = int(data1['Module_16'][0]['CV4'])
        print(f"Module_16 CV4 value: {module_16_cv4_value}")
        module_1_cv5_value = int(data1['Module_1'][0]['CV5'])
        print(f"Module_1 CV5 value: {module_1_cv5_value}")

        module_2_cv5_value = int(data1['Module_2'][0]['CV5'])
        print(f"Module_2 CV5 value: {module_2_cv5_value}")

        module_3_cv5_value = int(data1['Module_3'][0]['CV5'])
        print(f"Module_3 CV5 value: {module_3_cv5_value}")

        module_4_cv5_value = int(data1['Module_4'][0]['CV5'])
        print(f"Module_4 CV5 value: {module_4_cv5_value}")

        module_5_cv5_value = int(data1['Module_5'][0]['CV5'])
        print(f"Module_5 CV5 value: {module_5_cv5_value}")

        module_6_cv5_value = int(data1['Module_6'][0]['CV5'])
        print(f"Module_6 CV5 value: {module_6_cv5_value}")
        
        module_7_cv5_value = int(data1['Module_7'][0]['CV5'])
        print(f"Module_7 CV5 value: {module_7_cv5_value}")

        module_8_cv5_value = int(data1['Module_8'][0]['CV5'])
        print(f"Module_8 CV5 value: {module_8_cv5_value}")

        module_9_cv5_value = int(data1['Module_9'][0]['CV5'])
        print(f"Module_9 CV5 value: {module_9_cv5_value}")

        module_10_cv5_value = int(data1['Module_10'][0]['CV5'])
        print(f"Module_10 CV5 value: {module_10_cv5_value}")

        module_11_cv5_value = int(data1['Module_11'][0]['CV5'])
        print(f"Module_11 CV5 value: {module_11_cv5_value}")

        module_12_cv5_value = int(data1['Module_12'][0]['CV5'])
        print(f"Module_12 CV5 value: {module_12_cv5_value}")
        module_13_cv5_value = int(data1['Module_13'][0]['CV5'])
        print(f"Module_13 CV5 value: {module_13_cv5_value}")

        module_14_cv5_value = int(data1['Module_14'][0]['CV5'])
        print(f"Module_14 CV5 value: {module_14_cv5_value}")

        module_15_cv5_value = int(data1['Module_15'][0]['CV5'])
        print(f"Module_15 CV5 value: {module_15_cv5_value}")

        module_16_cv5_value = int(data1['Module_16'][0]['CV5'])
        print(f"Module_16 CV5 value: {module_16_cv5_value}")
        module_1_cv6_value = int(data1['Module_1'][0]['CV6'])
        print(f"Module_1 CV6 value: {module_1_cv6_value}")

        module_2_cv6_value = int(data1['Module_2'][0]['CV6'])
        print(f"Module_2 CV6 value: {module_2_cv6_value}")

        module_3_cv6_value = int(data1['Module_3'][0]['CV6'])
        print(f"Module_3 CV6 value: {module_3_cv6_value}")

        module_4_cv6_value = int(data1['Module_4'][0]['CV6'])
        print(f"Module_4 CV6 value: {module_4_cv6_value}")

        module_5_cv6_value = int(data1['Module_5'][0]['CV6'])
        print(f"Module_5 CV6 value: {module_5_cv6_value}")

        module_6_cv6_value = int(data1['Module_6'][0]['CV6'])
        print(f"Module_6 CV6 value: {module_6_cv6_value}")
        
        module_7_cv6_value = int(data1['Module_7'][0]['CV6'])
        print(f"Module_7 CV6 value: {module_7_cv6_value}")

        module_8_cv6_value = int(data1['Module_8'][0]['CV6'])
        print(f"Module_8 CV6 value: {module_8_cv6_value}")

        module_9_cv6_value = int(data1['Module_9'][0]['CV6'])
        print(f"Module_9 CV6 value: {module_9_cv6_value}")

        module_10_cv6_value = int(data1['Module_10'][0]['CV6'])
        print(f"Module_10 CV6 value: {module_10_cv6_value}")

        module_11_cv6_value = int(data1['Module_11'][0]['CV6'])
        print(f"Module_11 CV6 value: {module_11_cv6_value}")

        module_12_cv6_value = int(data1['Module_12'][0]['CV6'])
        print(f"Module_12 CV6 value: {module_12_cv6_value}")
        module_13_cv6_value = int(data1['Module_13'][0]['CV6'])
        print(f"Module_13 CV6 value: {module_13_cv6_value}")

        module_14_cv6_value = int(data1['Module_14'][0]['CV6'])
        print(f"Module_14 CV6 value: {module_14_cv6_value}")

        module_15_cv6_value = int(data1['Module_15'][0]['CV6'])
        print(f"Module_15 CV6 value: {module_15_cv6_value}")

        module_16_cv6_value = int(data1['Module_16'][0]['CV6'])
        print(f"Module_16 CV6 value: {module_16_cv6_value}")
        insert_query = """ INSERT INTO upscellvoltage(module_1_cv1_value,module_1_cv2_value,module_1_cv3_value,module_1_cv4_value,module_1_cv5_value,module_1_cv6_value,module_2_cv1_value,module_2_cv2_value,module_2_cv3_value,module_2_cv4_value,module_2_cv5_value,module_2_cv6_value,module_3_cv1_value,module_3_cv2_value,module_3_cv3_value,module_3_cv4_value,module_3_cv5_value,module_3_cv6_value,module_4_cv1_value,module_4_cv2_value,module_4_cv3_value,module_4_cv4_value,module_4_cv5_value,module_4_cv6_value,module_5_cv1_value,module_5_cv2_value,module_5_cv3_value,module_5_cv4_value,module_5_cv5_value,module_5_cv6_value,module_6_cv1_value,module_6_cv2_value,module_6_cv3_value,module_6_cv4_value,module_6_cv5_value,module_6_cv6_value,module_7_cv1_value,module_7_cv2_value,module_7_cv3_value,module_7_cv4_value,module_7_cv5_value,module_7_cv6_value,module_8_cv1_value,module_8_cv2_value,module_8_cv3_value,module_8_cv4_value,module_8_cv5_value,module_8_cv6_value,module_9_cv1_value,module_9_cv2_value,module_9_cv3_value,module_9_cv4_value,module_9_cv5_value,module_9_cv6_value,module_10_cv1_value,module_10_cv2_value,module_10_cv3_value,module_10_cv4_value,module_10_cv5_value,module_10_cv6_value,module_11_cv1_value,module_11_cv2_value,module_11_cv3_value,module_11_cv4_value,module_11_cv5_value,module_11_cv6_value,module_12_cv1_value,module_12_cv2_value,module_12_cv3_value,module_12_cv4_value,module_12_cv5_value,module_12_cv6_value,module_13_cv1_value,module_13_cv2_value,module_13_cv3_value,module_13_cv4_value,module_13_cv5_value,module_13_cv6_value,module_14_cv1_value,module_14_cv2_value,module_14_cv3_value,module_14_cv4_value,module_14_cv5_value,module_14_cv6_value,module_15_cv1_value,module_15_cv2_value,module_15_cv3_value,module_15_cv4_value,module_15_cv5_value,module_15_cv6_value,module_16_cv1_value,module_16_cv2_value,module_16_cv3_value,module_16_cv4_value,module_16_cv5_value,module_16_cv6_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (module_1_cv1_value,module_1_cv2_value,module_1_cv3_value,module_1_cv4_value,module_1_cv5_value ,module_1_cv6_value,module_2_cv1_value,module_2_cv2_value,module_2_cv3_value,module_2_cv4_value,module_2_cv5_value ,module_2_cv6_value,module_3_cv1_value,module_3_cv2_value,module_3_cv3_value,module_3_cv4_value,module_3_cv5_value ,module_3_cv6_value,module_4_cv1_value,module_4_cv2_value,module_4_cv3_value,module_4_cv4_value,module_4_cv5_value ,module_4_cv6_value,module_5_cv1_value,module_5_cv2_value,module_5_cv3_value,module_5_cv4_value,module_5_cv5_value ,module_5_cv6_value,module_6_cv1_value,module_6_cv2_value,module_6_cv3_value,module_6_cv4_value,module_6_cv5_value ,module_6_cv6_value,module_7_cv1_value,module_7_cv2_value,module_7_cv3_value,module_7_cv4_value,module_7_cv5_value ,module_7_cv6_value,module_8_cv1_value,module_8_cv2_value,module_8_cv3_value,module_8_cv4_value,module_8_cv5_value ,module_8_cv6_value,module_9_cv1_value,module_9_cv2_value,module_9_cv3_value,module_9_cv4_value,module_9_cv5_value ,module_9_cv6_value,module_10_cv1_value,module_10_cv2_value,module_10_cv3_value,module_10_cv4_value,module_10_cv5_value ,module_10_cv6_value,module_11_cv1_value,module_11_cv2_value,module_11_cv3_value,module_11_cv4_value,module_11_cv5_value ,module_11_cv6_value,module_12_cv1_value,module_12_cv2_value,module_12_cv3_value,module_12_cv4_value,module_12_cv5_value ,module_12_cv6_value,module_13_cv1_value,module_13_cv2_value,module_13_cv3_value,module_13_cv4_value,module_13_cv5_value ,module_13_cv6_value,module_14_cv1_value,module_14_cv2_value,module_14_cv3_value,module_14_cv4_value,module_14_cv5_value ,module_14_cv6_value,module_15_cv1_value,module_15_cv2_value,module_15_cv3_value,module_15_cv4_value,module_15_cv5_value ,module_15_cv6_value,module_16_cv1_value,module_16_cv2_value,module_16_cv3_value,module_16_cv4_value,module_16_cv5_value ,module_16_cv6_value)
        cursor.execute(insert_query, record_to_insert)
        conn.commit()
        
         
    if message.topic == Class_Definition.Config.config['Topics']['CT'] :
         print(data1)
         module_1_cv1_value = int(data1['Module_1'][0]['CT1'])
         print(f"Module_1 CV1 value OF CT: {module_1_cv1_value}")
 
         module_2_cv1_value = int(data1['Module_2'][0]['CT1'])
         print(f"Module_2 CV1 value of CT : {module_2_cv1_value}")

         module_3_cv1_value = int(data1['Module_3'][0]['CT1'])
         print(f"Module_3 CV1 value of CT: {module_3_cv1_value}")

         module_4_cv1_value = int(data1['Module_4'][0]['CT1'])
         print(f"Module_4 CV1 value OF CT: {module_4_cv1_value}")
         module_1_cv2_value = int(data1['Module_1'][0]['CT2'])
         print(f"Module_1 CV2 value OF CT: {module_1_cv2_value}")
 
         module_2_cv2_value = int(data1['Module_2'][0]['CT2'])
         print(f"Module_2 CV2 value of CT : {module_2_cv2_value}")

         module_3_cv2_value = int(data1['Module_3'][0]['CT2'])
         print(f"Module_3 CV2 value of CT: {module_3_cv2_value}")

         module_4_cv2_value = int(data1['Module_4'][0]['CT2'])
         print(f"Module_4 CV2 value OF CT: {module_4_cv2_value}")
         module_1_cv3_value = int(data1['Module_1'][0]['CT3'])
         print(f"Module_1 CV3 value OF CT: {module_1_cv3_value}")
 
         module_2_cv3_value = int(data1['Module_2'][0]['CT3'])
         print(f"Module_2 CV3 value of CT : {module_2_cv3_value}")

         module_3_cv3_value = int(data1['Module_3'][0]['CT3'])
         print(f"Module_3 CV3 value of CT: {module_3_cv3_value}")

         module_4_cv3_value = int(data1['Module_4'][0]['CT3'])
         print(f"Module_4 CV3 value OF CT: {module_4_cv3_value}")
         module_1_cv4_value = int(data1['Module_1'][0]['CT4'])
         print(f"Module_1 CV4 value OF CT: {module_1_cv4_value}")
 
         module_2_cv4_value = int(data1['Module_2'][0]['CT4'])
         print(f"Module_2 CV4 value of CT : {module_2_cv4_value}")

         module_3_cv4_value = int(data1['Module_3'][0]['CT4'])
         print(f"Module_3 CV4 value of CT: {module_3_cv4_value}")

         module_4_cv4_value = int(data1['Module_4'][0]['CT4'])
         print(f"Module_4 CV4 value OF CT: {module_4_cv4_value}")
         insert_query = """ INSERT INTO EMSUPSCellTemperature(module_1_cv1_value,module_1_cv2_value,module_1_cv3_value,module_1_cv4_value,module_2_cv1_value,module_2_cv2_value,module_2_cv3_value,module_2_cv4_value,module_3_cv1_value,module_3_cv2_value,module_3_cv3_value,module_3_cv4_value,module_4_cv1_value,module_4_cv2_value,module_4_cv3_value,module_4_cv4_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (
    module_1_cv1_value, module_1_cv2_value, module_1_cv3_value, module_1_cv4_value,
    module_2_cv1_value, module_2_cv2_value, module_2_cv3_value, module_2_cv4_value,
    module_3_cv1_value, module_3_cv2_value, module_3_cv3_value, module_3_cv4_value,
    module_4_cv1_value, module_4_cv2_value, module_4_cv3_value, module_4_cv4_value
)

         cursor.execute(insert_query, record_to_insert)
         conn.commit()

         
         

         
    if message.topic == Class_Definition.Config.config['Topics']['Specs'] :
         print(data1)
         packconfiguration = data1["CONFG"]
         celltotalsequencenumberspecs = data1["CTSEQ"]
         temperaturetotalsequencenumber = data1["TTSEQ"]
         softwareversion = data1["SVER"]
         hardwareversion = data1["HVER"]
         bmsversion = data1["BVER"]
         insert_query = """ INSERT INTO EMSUPSspecs(packconfigurationin_mXcXt,celltotalsequencenumber,temperaturetotalsequencenumber,softwareversion,hardwareversion,bmsversion) VALUES (%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (packconfiguration,celltotalsequencenumberspecs,temperaturetotalsequencenumber,softwareversion,hardwareversion,bmsversion)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()

while True:
    try:
        client = mqtt.Client()
        #client.username_pw_set(username="admin",password="admin@123")
        #client.connect("10.9.211.140",1883,60)
        client.username_pw_set(username=Class_Definition.Config.config['Broker_connection']['userName'],password=Class_Definition.Config.config['Broker_connection']['password'])
        client.connect(Class_Definition.Config.config['Broker_connection']['broker'],Class_Definition.Config.config['Broker_connection']['port'], Class_Definition.Config.config['Broker_connection']['keep_alive'])
        client.on_connect = on_connect
        client.on_message = on_message
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        break
    except Exception as ex:
        print("connection lost")
        print(ex)
        time.sleep(10)
        client.reconnect()
