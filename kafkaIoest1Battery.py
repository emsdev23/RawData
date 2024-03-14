from kafka import KafkaConsumer
import time
from collections import deque
from datetime import datetime
import mysql.connector


consumer = KafkaConsumer(
        'ioeBattery',  # Specify the topic(s) to subscribe to
        bootstrap_servers='43.205.196.66:9092',  # Specify the Kafka server(s)  # Specify the consumer group ID
        # auto_offset_reset='earliest',  # Start consuming messages from the earliest offset
        enable_auto_commit=True,  # Automatically commit offsets
        auto_commit_interval_ms=1000,  # Auto commit interval in milliseconds
    )

message_buffer = deque(maxlen=5)

total_li = []

for message in consumer:

    unprocesseddb = mysql.connector.connect(
                host="121.242.232.211",
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
    
    ltocur = unprocesseddb.cursor()

    hex_data = message.value.decode('utf-8')
    if len(hex_data) > 0:
        print(hex_data)

        def convertLTOer(cleaned_li):
            if cleaned_li[0] == "13":
                # print(cleaned_li[3:])
                now = datetime.now()
                total_li.append(cleaned_li)
                # ltoBatteryEnergy(cleaned_li[3:],str(now)[0:-7])
            if cleaned_li[0] == "03":
                now = datetime.now()
                total_li.append(cleaned_li)
                # ltoBattery(cleaned_li[3:],str(now)[0:-7])

        def clean_resper(raw_li):
            li = []
            order_li = raw_li.split(" ")
            for i in order_li:
                if len(i) > 1:
                    li.append(i)
                if len(li) > 1:
                    # print(li)
                    convertLTOer(li)

        hex_data = message.value.decode('utf-8')

        if hex_data[9:11] == '11':
            initial_li = hex_data.split('88 18')
        else:
            initial_li = []

        if len(initial_li) > 0:

            for i in initial_li:
                print(i)
                clean_resper(i)

        hex_data2 = ""
        for prev_message in message_buffer:
            hex_data2 = prev_message.value.decode('utf-8')

        if len(hex_data2)>0:
            if hex_data2[9:11] == '11':
                initial_li1 = hex_data2.split('88 18')
            else:
                initial_li1 = []

            if len(initial_li1) > 0:
                for i in initial_li1:
                    print(i)
                    clean_resper(i)

    def filter_lists(hex_lists):
        list_03 = []
        list_13 = []

        for inner_list in hex_lists:
            if inner_list[0] == '03':
                list_03.append(inner_list)
            elif inner_list[0] == '13':
                list_13.append(inner_list)

        return list_03, list_13
    
    batteryStsDict = {}
    batteryli = []
    def ltoBattery(clean_li,rectime):
                    # print(clean_li)
        try:
            batteryVolt = clean_li[1] + clean_li[0]
            batteryVolt = int(batteryVolt,16)/10
        except Exception as ex:
                        # print(ex)
            batteryVolt = None
                    
        try:
            batteryCurent = clean_li[3] + clean_li[2]
            batteryCurent = int(batteryCurent,16) / 100
        except:
            batteryCurent = None

                    # print("bat cur",batteryCurent)
        try:
            mainConsSts = clean_li[4][1]
            preConSts = clean_li[4][0]
                        # print("main sts",mainConsSts)
                        # print("prests",preConSts)
        except:
            mainConsSts = None
            preConSts = None

        try:
            batterySts = clean_li[5][1]
                        # print(batteryCurent)
                                # CHG -> 3 , DCHG -> 2 ,
                        # print("sts",batterySts)
            if batterySts == '2':
                batterySts = 'IDLE'
            elif batterySts == '3':
                batterySts = 'CHG'
            elif batterySts == '4':
                            # print(batteryCurent)
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
            print(usableSoc)
        except:
            usableSoc = None
                    
        batteryStsDict['batteryVolt'] = batteryVolt
        batteryStsDict['batteryCurent']  = batteryCurent
        batteryStsDict['mainConsSts'] = mainConsSts
        batteryStsDict['preConSts'] = preConSts
        batteryStsDict['batterySts'] = batterySts
        batteryStsDict['packSoc'] = packSoc
        batteryStsDict['usableSoc'] = usableSoc

                    # print(rectime[0:17])

        if int(rectime[17:]) >= 0  and int(rectime[17:]) < 15:
            batteryStsDict['rectime']=rectime[0:17]+"00"
        elif int(rectime[17:]) >= 15   and int(rectime[17:]) < 30:
            batteryStsDict['rectime']=rectime[0:17]+"15"
        elif int(rectime[17:]) >= 30  and int(rectime[17:]) < 45:
            batteryStsDict['rectime']=rectime[0:17]+"30"
        elif int(rectime[17:]) >= 45  and int(rectime[17:]) < 59:
            batteryStsDict['rectime']=rectime[0:17]+"45"

    chgstsli = []
    def ltoBatteryEnergy(clean_li,rectime):
        try:
            chargingEnergy = clean_li[1]+clean_li[0]
                        # print(chargingEnergy)
            chargingEnergy = int(chargingEnergy,16)
        except:
            chargingEnergy =  None

        try:
            dischargingEnergy = clean_li[3]+clean_li[2]
                        # print(dischargingEnergy)
            dischargingEnergy = int(dischargingEnergy,16)
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

        batteryStsDict['chargingEnergy'] = chargingEnergy
        batteryStsDict['dischargingEnergy'] = dischargingEnergy
        batteryStsDict['availableEnergy'] = availableEnergy
    
    if len(total_li)> 2:
        list_03, list_13 = filter_lists(total_li)

        if len(list_03)>0 and len(list_13) > 0:
            now = str(datetime.now())[0:-7]
            ltoBattery(list_03[0][3:],now)
            ltoBatteryEnergy(list_13[0][3:],now)
        
    print(batteryStsDict)

    try:
        val = (batteryStsDict['batteryVolt'],batteryStsDict['batteryCurent'],batteryStsDict['mainConsSts'],batteryStsDict['preConSts'],batteryStsDict['batterySts'],batteryStsDict['packSoc'],batteryStsDict['usableSoc'],batteryStsDict['chargingEnergy'],batteryStsDict['dischargingEnergy'],batteryStsDict['availableEnergy'],batteryStsDict['rectime'])
    except Exception as ex:
        print(ex)
        ltocur.close()
        continue
    sql = "INSERT INTO ioeSt1BatteryData(batteryVoltage,batteryCurrent,mainContactorStatus,prechargeContactorStatus,batteryStatus,packSOC,packUsableSOC,chargingEnergy,dischargingEnergy,availableEnergy,recordTimestamp) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    print(val)
    try:
        ltocur.execute(sql,val)
        unprocesseddb.commit()
                # print(val)
        print("IOE String1 Battery data inserted")
        ltocur.close()
        unprocesseddb.close()
    except Exception as ex:
        print("Data not inserted")
        print(ex)

    time.sleep(30)

consumer.close()
