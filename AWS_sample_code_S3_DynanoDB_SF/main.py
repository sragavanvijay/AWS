#import yaml
import json
import os
import sys
import requests
import boto3
import collections
import pandas as pd
from csv import DictWriter
from botocore.exceptions import ClientError
from datetime import date, datetime
from pathlib import Path
from io import BytesIO
from config import *
from simple_salesforce import Salesforce


#from simple_salesforce import Salesforce, SFType, SalesforceMalformedRequest

def get_dynamodb_connection():
    return boto3.resource('dynamodb', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name)

def get_s3_connection():
    return boto3.resource('s3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name)



def main():
    
    # Creating instance of dynamoDB
    dynamodb=get_dynamodb_connection()
    s3 = get_s3_connection()

    my_s3_bucket = s3.Bucket('rtm-personalisation')
    
    # printing all the objects in S3 bucket
    #for file in my_s3_bucket.objects.all():
    #    print('\n')
    #    print("---------")
    #    print(file.key)

    # Create the DynamoDB table.
    #table = dynamodb.create_table(
    #    TableName='users_1',
    #    KeySchema=[
    #        {
    #            'AttributeName': 'AccountId',
    #            'KeyType': 'HASH'
    #        }
    #    ],
    #    AttributeDefinitions=[
    #        {
    #            'AttributeName': 'AccountId',
    #            'AttributeType': 'S'
    #        }
    #    ],
    #    ProvisionedThroughput={
    #        'ReadCapacityUnits': 5,
    #        'WriteCapacityUnits': 5
    #    }
    #)

    ##creating an instance of "Users" Table
    table1 = dynamodb.Table('JJ_RTM_Events')

    #printing table stats:
    print(table1.creation_date_time)

    #Extracing data out of dynamoDB table

    #response = table1.get_item(
    #    Key={
    #        'username': 'janedoe_1',
    #    }
    #)
    #item = response['Item']
#
    #j_item = json.loads(item)
    #print(item)


    # initiating a SF instance 
    sf = Salesforce(username='vsam@deloitte.com.au.janssenapacdd', password='Goodmorning@1', security_token='qAWxhSDi5UcWjwrNmhh6ZmBhN')

    #Query to be executed:
    sf_contacts_table = sf.query("select AccountId,ActivityDate,CallDisposition,CallDurationInSeconds,CallObject,CallType,CreatedById,CreatedDate,Description,Id,IsArchived,IsClosed,IsDeleted,IsHighPriority,IsRecurrence,IsReminderSet,LastModifiedById,LastModifiedDate,OwnerId,Priority,RecurrenceActivityId,RecurrenceDayOfMonth,RecurrenceDayOfWeekMask,RecurrenceEndDateOnly,RecurrenceInstance,RecurrenceInterval,RecurrenceMonthOfYear,RecurrenceRegeneratedType,RecurrenceStartDateOnly,RecurrenceTimeZoneSidKey,RecurrenceType,ReminderDateTime,Status,Subject,SystemModstamp,TaskSubtype,WhatId,WhoId FROM Task ")
    #"select AccountId, CallType, Description, Id, OwnerId, Subject,WhoId,TaskSubtype, Status,  WhatId  FROM Task WHERE WhoId = '0032x000001tPcSAAU' ")
    
    #converting the query reselts to a pandas df:
    sf_df = pd.DataFrame(sf_contacts_table['records']).drop(columns='attributes')

    #adding "event_it" an combinational hash of "whoId", "Id", "OwnerId"
    sf_df['event_id'] = (sf_df['WhoId'] + sf_df['Id'] + sf_df['OwnerId']).apply(hash)

    #Ordering the columns of the DF
    #sf_df = sf_df[['event_id','AccountId','Status','Description','Subject','WhoId','WhatId','Id','OwnerId','TaskSubtype','CallType']]
    
    #coverting all the columns as string
    sf_df = sf_df.applymap(str)
    
    
    #Converting DF to Json
    json_data= sf_df.to_json(orient='records', lines=True)
    
    print(json_data)    

    #Converting  <class 'str'> to <class 'dict'>
    #data_to_load = json.loads(json_data)
    

    #for line in json_data:
    #    print(line)
    #    print('\n')


    #data_to_load = [json.loads(line) for line in a ] 
    print("--->")
    print (data_to_load)

    #data_to_load = json.loads(json_data) 

    # inserting data into dynamoDB table:
    #table1.put_item(Item=data_to_load)

# Wait until the table exists.
    #table.meta.client.get_waiter('table_exists').wait(TableName='users')

# Print out some data about the table.
    #print(table.item_count)
    #print(table.creation_date_time)



if __name__ == "__main__":
    main()