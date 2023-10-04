import json
import boto3
import os
import logging
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) #Options: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET

def lambda_handler(event, context):
    logger.info(f"Event: {event}")
    
    tableName = os.environ['tableName']
    contactFlow = event['Details']['Parameters']['contactFlow']
    lang = event['Details']['Parameters']['lang']
    returnDict = {}
    returnDict.update(event['Details']['Parameters'])
    
    table = boto3.resource('dynamodb', region_name="us-east-1").Table(tableName)
    
    queries = contactFlow.split("|")
    
    for query in queries:
        configInfo = table.scan(
            FilterExpression=Key("key").eq(query)
        )['Items']
        
        for entry in configInfo:
            logger.info(f"Entry: {entry}")
            if 'data' in entry:
                key = entry['name']
                value = entry['data']
                if type(value) is list:
                    for x in value:
                        if x['lang'] == lang:
                            if 'value' in x:
                                value = x['value']
                            else:
                                logger.warning(f"Prompt {key} is empty")
                                value = ''
                item = {key: value}
                returnDict.update(item)
    
    #sessionAttributes = json.dumps(returnDict).replace('"', '\"').replace(': ', ':').replace("\", ",  "\",").replace("\\u2028", "\u2028").replace("\\u00a0", " ")
    sessionAttributes = str(json.dumps(returnDict)).replace(': ', ':').replace("\", ",  "\",")
    
    returnDict['sessionAttributes'] = sessionAttributes
    
    return returnDict