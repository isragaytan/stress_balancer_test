from datetime import datetime
from datetime import timedelta
import os
import multiprocessing
import time

import pymysql
import pandas as pd
from datetime import timedelta

from datetime import datetime


from botocore.exceptions import ClientError


#ENVIRONMENT VARIABLES
# Get environment variables
DB_HOST_WIWI_MS= os.environ.get('DB_HOST_WIWI_MS')
DB_USER_WIWI_MS=os.environ.get('DB_USER_WIWI_MS')
DB_PASSWORD_WIWI_MS = os.environ.get('DB_PASSWORD_WIWI_MS')
DB_DATABASE_NAME_WIWI_MS = os.environ.get('DB_DATABASE_NAME_WIWI_MS')

AWS_KEY_WIWI_MS = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_WIWI_MS =os.environ.get('AWS_SECRET_ACCESS_KEY')

#CONNECTION
cnx = pymysql.connect(host=DB_HOST_WIWI_MS,
                             user=DB_USER_WIWI_MS,
                             
                             password=DB_PASSWORD_WIWI_MS ,
                             database=DB_DATABASE_NAME_WIWI_MS,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor,
                             connect_timeout=3153600,read_timeout=3153600,write_timeout=3153600
                             )

#MAC TESTS
arr_macs_hard= ["4C-91-0C-5A-0E-6C","DC-BF-E9-2D-09-50","C2-09-89-CF-C8-28","44-C7-FC-FB-DD-8E","B8-94-E7-D1-95-6A"]

#Rad test caller
#radtest -x  F2-C0-97-A0-52-C5 F2-C0-97-A0-52-C5 52.70.127.105 10 z0w2sIF06m
def rad_test(arr_macs):
    
    print("IN RAD TEST",arr_macs)
    
    str_execute = "radtest -x " + arr_macs + " " + arr_macs + "  3.14.125.157 10 z0w2sIF06m"
    print(str_execute) 
    stream = os.popen('radtest -x ' + arr_macs + " " + arr_macs + '  3.14.125.157 10 z0w2sIF06m')
    output = stream.read()
    print(output)
    
    #stream = os.popen('radtest -x F2-C0-97-A0-52-C5 F2-C0-97-A0-52-C5 52.70.127.105 10 z0w2sIF06m')
                        #radtest -x B2-10-F8-65-83-23 B2-10-F8-65-83-2310 z0w2sIF06m
    #output = stream.read()
    #print(output)

#Get valid ids
def get_first_timestamp():
    print("Trying to get valid macs")
    try:
        
        sql ="select * from pre_auth order by fecha DESC limit 1;"
    
        macs = pd.read_sql(sql,cnx)
        print(macs)
        
        #print(macs["fecha"][0])
        
        
        f_time = get_last_date(macs["fecha"][0])
        
        arr_tb =get_time_block(f_time)

        print(arr_tb)
        
        start = time.perf_counter()
        
        #processes = [multiprocessing.Process(target=rad_test, args=[arr_macs]) for arr_macs in arr_macs_hard]
        
        processes = [multiprocessing.Process(target=rad_test, args=[arr_macs]) for arr_macs in arr_tb]

        # start the processes
        for process in processes:
            process.start()

        # wait for completion
        for process in processes:
            process.join()

        finish = time.perf_counter()
        
        len_proc = len(arr_tb)
        
        print(f'Taked  {finish-start: .2f} second(s) to finish and processed ' ,str(len_proc))
        #rad_test(arr_tb)
        
    except Exception as ex:
        
        print(ex)

#Get one date and go 30 minutes back for select ids
def get_last_date(str_date):
    date_string = str(str_date)
    date_format = "%Y-%m-%d %H:%M:%S"
    given_time = datetime.strptime(date_string, date_format)
    
    print('Given Timestamp: ', given_time)  
    final_time = given_time - timedelta(minutes=30)
    
    print('Final Time (2 minutes ahead of given time ): ', final_time)
    return final_time
    
#Get blocks between time
def get_time_block(str_date1):
    print("GETTING BLOCKS")
    arr_dates = []
    try:
        
        sql ="select mac from pre_auth where fecha >= '%s'" %(str_date1)
        print("SQL", sql)
    
        valid_macs = pd.read_sql(sql,cnx)
        
       #print(valid_dates)
        for j in valid_macs['mac']:
           print(str(j))
           arr_dates.append(str(j))
        
    except Exception as ex:
        
        print(ex)
        
    return arr_dates

    

if __name__ == "__main__":
    get_first_timestamp()