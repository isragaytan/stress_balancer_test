from datetime import datetime
from datetime import timedelta
import os

import pymysql
import os 
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

#Get valid ids
def get_first_timestamp():
    print("Trying to get valid macs")
    try:
        
        sql ="select * from pre_auth order by fecha DESC limit 1;"
    
        macs = pd.read_sql(sql,cnx)
        
        print(macs["fecha"][0])
        
        
        f_time = get_last_date(macs["fecha"][0])
        
        arr_tb =get_time_block(f_time)
        
        rad_test(arr_tb)
        
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
        
        sql ="select fecha from pre_auth where fecha >= '%s' limit 50" %(str_date1)
        print("SQL", sql)
    
        valid_dates = pd.read_sql(sql,cnx)
        
       #print(valid_dates)
        for j in valid_dates['fecha']:
           print(str(j))
           arr_dates.append(str(j))
        
    except Exception as ex:
        
        print(ex)
        
    return arr_dates
        
#Rad test caller
def rad_test(arr_dates):
    print("IN RAD TEST",arr_dates)

if __name__ == "__main__":
    get_first_timestamp()