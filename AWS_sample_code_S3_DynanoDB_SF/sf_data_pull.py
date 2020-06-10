import boto3
import sys
import json
import os
from config import *
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce



def sf_data_pull(event,context):
    dynamodb=boto3.resource('dynamodb',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)
	
    print('Event data - ')
    print(event)
    
    sf = Salesforce(username='vsam@deloitte.com.au.janssenapacdd', password='Goodmorning@1', security_token='qAWxhSDi5UcWjwrNmhh6ZmBhN')

    #Query to be executed:
    sf_contacts_table = sf.query("select AccountId, CallType, Description, Id, OwnerId, PriorityStatus, Subject,WhoId,TaskSubtype, Status,  WhatId FROM Task WHERE WhoId = '0032x000001tPcSAAU' limit 1 ")
    
    #converting the query reselts to a pandas df:
    sf_df = pd.DataFrame(sf_contacts_table['records']).drop(columns='attributes')

    #adding "event_it" an combinational hash of "whoId", "Id", "OwnerId"
    sf_df['event_id'] = (sf_df['WhoId'] + sf_df['Id'] + sf_df['OwnerId']).apply(hash)

    #Ordering the columns of the DF
    sf_df = sf_df[['event_id','AccountId','Status','Description','Subject','WhoId','WhatId','Id','OwnerId','TaskSubtype','CallType', 'PriorityStatus']]
    
    #coverting all the columns as string
    sf_df = sf_df.applymap(str)

    #Converting DF to Json
    json_data= sf_df.to_json(orient='records', lines=True)

    #Converting  <class 'str'> to <class 'dict'>
    data_to_load = json.loads(json_data) 

	#creating Dynamo DB connection
		
	#creating a instance of "JJ_RTM_Events" table
    table1 = dynamodb.Table('JJ_RTM_Events')
	
    # inserting data into dynamoDB table:
    table1.put_item(Item=data_to_load)
	
    
    print('Additional processing below')
    
