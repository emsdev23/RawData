# 
import socket
import struct
from datetime import datetime
import mysql.connector
import time

while True:
    # Define the server's IP address and port
    server_ip = "10.9.244.1"  # Replace with your server's IP address
    server_port = 15153  # Replace with your server's Modbus port number

    # Define the Modbus unit ID (slave address)
    unit_id = 2  # Replace with your specific unit ID

    # Modbus function code for reading holding registers
    read_holding_registers = 3  # Function code 3 for reading holding registers

    # Register address to read (e.g., address 0)
    register_address = 0

    hex_data = "418000000006020300000014"
    request_packet = bytes.fromhex(hex_data)

    # Number of registers to read
    num_registers_to_read = 20

    # Create a TCP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to the Modbus TCP server
        try:
            client_socket.connect((server_ip, server_port))
        except:
            time.sleep(10)
            continue
        
        unprocesseddb = mysql.connector.connect(
            host="121.242.232.151",
            user="ltouser",
            password="ltouser@151",
            database='bmsmgmtprodv13',
            port=3306
        )

        # unprocesseddb = mysql.connector.connect(
        #         host="121.242.232.211",
        #         user="emsroot",
        #         password="22@teneT",
        #         database='EMS',
        #         port=3306
        #     )

        ltocur = unprocesseddb.cursor()

        # Receive the response from the server
        response = client_socket.recv(10240) 

        hex_string = ' '.join(f"{byte:02X}" for byte in response)

        print(hex_string)
        
        chgstsli = []
        def ltoBatteryEnergy(clean_li,rectime):
            try:
                chargingEnergy = clean_li[1]+clean_li[0]
                chargingEnergy = int(chargingEnergy,16) / 100
            except:
                chargingEnergy =  None

            try:
                dischargingEnergy = clean_li[3]+clean_li[2]
                dischargingEnergy = int(dischargingEnergy,16) / 100
            except:
                dischargingEnergy = None

            try:
                availableEnergy = clean_li[5]+clean_li[4]
                availableEnergy = int(availableEnergy,16)
            except:
                availableEnergy = None

            chgstsli.append(chargingEnergy)
            chgstsli.append(dischargingEnergy)
            chgstsli.append(availableEnergy)
        
        batteryli = []
        def ltoBattery(clean_li,rectime):
            # print(clean_li)
            try:
                batteryVolt = clean_li[1] + clean_li[0]
                batteryVolt = int(batteryVolt,16)/10
            except:
                batteryVolt = None
            
            try:
                batteryCurent = clean_li[3] + clean_li[2]
                batteryCurent = int(batteryCurent,16) / 100
            except:
                batteryCurent = None

            try:
                mainConsSts = clean_li[4][0]
                preConSts = clean_li[4][1]
                # print("main sts",mainConsSts)
                # print("prests",preConSts)
            except:
                mainConsSts = None
                preConSts = None

            try:
                batterySts = clean_li[5][0]
                # print(batteryCurent)
                # CHG -> 3 , DCHG -> 2 ,
                # print(batterySts)
                if batterySts == '2':
                    batterySts = 'IDLE'
                elif batterySts == '3':
                    batterySts = 'CHG'
                elif batterySts == '4':
                    if batteryCurent > 3:
                        batterySts = 'DCHG'
                    else:
                        batterySts = 'IDLE'
                elif batterySts == '5':
                    batterySts = 'FAULT'
            
            except:
                batterySts = None
            
            try:
                packSoc = clean_li[6]
                packSoc = int(packSoc,16)
            except:
                packSoc = None
            
            try:
                usableSoc = int(clean_li[7],16)
            except:
                usableSoc = None
            
            batteryli.append(batteryVolt)
            batteryli.append(batteryCurent)
            batteryli.append(mainConsSts)
            batteryli.append(preConSts)
            batteryli.append(batterySts)
            batteryli.append(packSoc)
            batteryli.append(usableSoc)
            batteryli.append(rectime)
            # print(batteryli)
            # print(chgstsli)
            # print(batteryCurent)
            # print((batteryVolt,batteryCurent,mainConsSts,preConSts,batterySts,packSoc,usableSoc,rectime))
        
        def convertLTO(cleaned_li):
            if cleaned_li[1] == "13":
                # print(cleaned_li[0:4])
                now = datetime.now()
                ltoBatteryEnergy(cleaned_li[4:],str(now)[0:-7])
            if cleaned_li[1] == "03":
                now = datetime.now()
                ltoBattery(cleaned_li[4:],str(now)[0:-7])
 
        def clean_resp(raw_li):
            li = []
            order_li = raw_li.split(" ")
            for i in order_li:
                if len(i) > 1:
                    li.append(i)
            if len(li) > 1:
                # print(li)
                convertLTO(li)
        
        initial_li = hex_string.split('88')

        for i in initial_li:
            clean_resp(i)
        
        # finli = chgstsli + batteryli
        # print(finli)
        if len(batteryli) >= 8:
            batVoltage = batteryli[0]
            batCurrent = batteryli[1]
            mainCon = batteryli[2]
            preCon = batteryli[3]
            batSts = batteryli[4]
            packSoc = batteryli[5]
            usableSoc = batteryli[6]
            received_time = batteryli[7]

            sql = "INSERT INTO ltoBatteryData(batteryVoltage,batteryCurrent,mainContactorStatus,prechargeContactorStatus,batteryStatus,packSOC,packUsableSOC,recordTimestamp) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (batVoltage,batCurrent,mainCon,preCon,batSts,packSoc,usableSoc,received_time)
            print(val)
            try:
                ltocur.execute(sql,val)
                unprocesseddb.commit()
                # print(val)
                print("LTO Battery data inserted")
            except Exception as ex:
                print("Data not inserted")
                print(ex)

        # print(chgstsli)
        if len(chgstsli) >= 3:
            # print(finli)
            chgEnergy = chgstsli[0] / 100
            dchgEnergy = chgstsli[1] / 100
            availEnergy = chgstsli[2] / 100

            #chargingEnergy,dischargingEnergy,availableEnergy
            #chgEnergy,dchgEnergy,availEnergy
            
            sql = "INSERT INTO ltoBatteryData(chargingEnergy,dischargingEnergy,availableEnergy,recordTimestamp) values(%s,%s,%s,%s)"
            val = (chgEnergy,dchgEnergy,availEnergy,received_time)
            try:
                ltocur.execute(sql,val)
                unprocesseddb.commit()
                print(val)
                print("LTO Battery data inserted")
            except mysql.connector.errors.IntegrityError:
                sql = "update ltoBatteryData SET chargingEnergy = %s,dischargingEnergy = %s,availableEnergy = %s where recordTimestamp = %s"
                val = (chgEnergy,dchgEnergy,availEnergy,received_time)
                ltocur.execute(sql,val)
                unprocesseddb.commit()
                print(val)
                print("LTO battery data inserted")

    finally:
        # Close the socket
        client_socket.close()

    time.sleep(14)
    