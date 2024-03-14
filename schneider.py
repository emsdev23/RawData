from pymongo import MongoClient 
import time
import mysql.connector
import pytz
from datetime import datetime

format = "%Y-%m-%d %H:%M:%S"
original_tz = pytz.timezone('Asia/Kolkata')
datetime_object = datetime.now()
dated = datetime_object.strftime(format)[0:10]

client = MongoClient("43.205.196.66", 27017)

emsmongodb = client['ems']

while True:

    try:
        processeddb = mysql.connector.connect(
		host="localhost",
		user="emsrouser",
		password="emsrouser@151",
		database='bmsmgmt_olap_prod_v13',
		port=3306
		)
    except Exception as ex:
        print(ex)
        time.sleep(10)
        continue

    emsdb = mysql.connector.connect(
        host="121.242.232.211",
        user="emsroot",
        password="22@teneT",
        database='EMS',
        port=3306
    )
    
    awsdb = mysql.connector.connect(
        host="43.205.196.66",
        user="emsroot",
        password="22@teneT",
        database='EMS',
        port=3307
    )

    try:
        proscur = processeddb.cursor()
    except Exception as ex:
        print(ex)
        time.sleep(20)
        continue
    
    try:
        emscur = emsdb.cursor()
    except Exception as ex:
        print(ex)
        time.sleep(15)
        continue
    
    try:
        awscur = awsdb.cursor()
    except Exception as ex:
        print(ex)
        time.sleep(15)
        continue

    emscur.execute("SELECT polledTime FROM bmsunprocessed_prodv13.hvacSchneider7230Polling order by polledTime desc limit 1")

    res = emscur.fetchall()
    
    awscur.execute("SELECT polledTime FROM bmsunprocessed_prodv13.hvacSchneider7230Polling order by polledTime desc limit 1")

    res1 = awscur.fetchall()

    try:
        lastdate = res[0][0]
    except:
        lastdate = None
    
    try:
        lastdate1 = res1[0][0]
    except:
        lastdate1 = None

    
    #EMSdb
    if lastdate != None:
        proscur.execute(f"SELECT recordId,polledTime,totalApparentPower1,totalApparentPower2,remark from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where polledTime > '{lastdate}'")
       
        data = proscur.fetchall()
        if len(data) !=0:
            for i in data:
                sql = "INSERT INTO bmsunprocessed_prodv13.hvacSchneider7230Polling(recordId,polledTime,totalApparentPower1,totalApparentPower2,remark) values(%s,%s,%s,%s,%s)"
                if i[2]!= None and i[2] < 0:
                    val = (i[0],i[1],0,i[3],i[4])
                else:   
                    val = (i[0],i[1],i[2],i[3],i[4])
                print(val)
                emscur.execute(sql,val)
                emsdb.commit()
                print("Schneider data inserted EMS")
    else:
        proscur.execute("SELECT recordId,polledTime,totalApparentPower1,totalApparentPower2,remark from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime)=curdate()")
       
        data = proscur.fetchall()
       
        for i in data:
            sql = "INSERT INTO bmsunprocessed_prodv13.hvacSchneider7230Polling(recordId,polledTime,totalApparentPower1,totalApparentPower2,remark) values(%s,%s,%s,%s,%s)"
            val = (i[0],i[1],i[2],i[3],i[4])
            emscur.execute(sql,val)
            emsdb.commit()
            print("Schneider data inserted EMS")
    
    #AWSdb        
    if lastdate1 != None:
        proscur.execute(f"SELECT recordId,polledTime,totalApparentPower1,totalApparentPower2,remark from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where polledTime > '{lastdate1}'")
       
        data = proscur.fetchall()
        if len(data) !=0:
            for i in data:
                sql = "INSERT INTO bmsunprocessed_prodv13.hvacSchneider7230Polling(recordId,polledTime,totalApparentPower1,totalApparentPower2,remark) values(%s,%s,%s,%s,%s)"
                if i[2]!= None and i[2] < 0:
                    val = (i[0],i[1],0,i[3],i[4])
                else:
                    val = (i[0],i[1],i[2],i[3],i[4])
                #print(val)
                awscur.execute(sql,val)
                awsdb.commit()
                print("Schneider data inserted AWS")
    else:
        proscur.execute("SELECT recordId,polledTime,totalApparentPower1,totalApparentPower2,remark from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime)=curdate()")
       
        data = proscur.fetchall()
       
        for i in data:
            sql = "INSERT INTO bmsunprocessed_prodv13.hvacSchneider7230Polling(recordId,polledTime,totalApparentPower1,totalApparentPower2,remark) values(%s,%s,%s,%s,%s)"
            val = (i[0],i[1],i[2],i[3],i[4])
            awscur.execute(sql,val)
            awsdb.commit()
            print("Schneider data inserted AWS")
    
    cur = emsmongodb.hvacSchneider7230.find().sort({'recordid':-1}).limit(1)

    rec_id = cur[0]['recordid']
    
    print(rec_id)

    proscur = processeddb.cursor()

    proscur.execute(f"SELECT recordId,polledTime,totalApparentPower1,totalApparentPower2,remark,date(polledTime) from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where recordid > {rec_id}")

    peakres = proscur.fetchall()

    for i in peakres:
        record = {'recordid':i[0],'polledTime':i[1],'totalApparentPower1':i[2],'totalApparentPower2':i[3],
                'remark':i[4],'polledDate':str(i[5])}
        
        print(record)

        emsmongodb.hvacSchneider7230.insert_one(record)

        print("Schneider inserted in MongoDB")

    proscur.close()
   
    print("SLEEP")

    time.sleep(20)
