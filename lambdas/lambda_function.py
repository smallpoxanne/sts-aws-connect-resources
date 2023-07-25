import json
import logging
import os
import boto3
import sys
import time
import csv
import decimal
import concurrent. futures
from tydirium import Tydirium
from queue import Queue
from botocore.exceptions import ClientError
import test_function as TestFunction
# import contact_flow as ContactFlow
# import contact_flow_module as ContactFlowModule
# import routing_profile as RoutingProfile
# import connect_queue as ConnectQueue
# import hours_of_operation as HoursOfOperation
# import contact_flow_lambda as ContactFlowLambda
# import dynamo_tables_v3 as DynamoTable
# import security_profile as Security Profile
# import contact_flow_lex as ContactFlowLex
# import agent_status as AgentStatus
# import agent_hierarchy as AgentHierarchy
# import quick_connect as QuickConnect
# import lambdas_get_diffs as LambdasGetDiffs
# import get_user_data as GetUserData
# import contact_flow_syncone as ContactFlowSyncone
# import create_s3_jsons as CreateS3Jsons
# global logger
# role = os.environ['role']

def lambda_handler(event, context):
  '''
  - This function is the entry and exit point of this lambda.
  - Gathers the config data from json files in their respective configurations folders
  - Runs threads to configure resources
  - Puts together output
  '''
  sourceEnv = event["source"]
  sourceArn=os.environ[sourceEnv+"Arn"]
  destinationEnv = event["destination"]
  destinationArn=os.environ[destinationEnv+"Arn"]
  logger = logging.getLogger(__name__)
  logger.setLevel(logging. INFO) #Options: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
  ts= time.time()
  # Initialize output variables and passing variables
  output = []
  q = Queue ()
  data = []
  
  logger.info('Boto3 Version: ')
  logger.info(boto3. __version__)
  sourceConnectClient, destinationConnectClient, sourceLambdaClient, destinationLambdaClient, sourceLexClient, destinationLexClient, sourceDynamoClient, destinationDynamoClient = get_clients(logger, ts, destinationEnv)
  # Agent Hierarchy takes a long time to run, so we are running it in a separate thread.
  # All others run together in their own thread as they need to be run in order
  with concurrent.futures.ThreadPoolExecutor() as executor:
    result = [
    # executor.submit(AgentStatus.sync_agent_statuses, logger, ts, source ConnectClient, destination ConnectClient)
    ]
    # ContactFlowSyncone.compare(logger, ts, sourceConnectClient, destinationConnectClient, source LexClient, destinationLexClient)
    TestFunction.test_function(logger, ts, sourceConnectClient, destinationConnectClient, sourceArn, destinationArn)
    
    for future in concurrent.futures.as_completed (result):
      logger.info(future)
  
def get_clients (logger, ts, destinationEnv):
  if(destinationEnv == 'stg' or destinationEnv == 'prd'):
    sourceConnectClient = Tydirium('connect', ts, os.environ['AssumeRole'], os.environ ['Region']).client 
    destinationConnectClient = boto3.client('connect')
    sourceLambdaClient = Tydirium('lambda', ts, os.environ['AssumeRole'], os.environ ['Region']).client 
    destinationLambdaClient = boto3.client('lambda')
    sourceLexClient = Tydirium('lexv2-models', ts, os.environ['AssumeRole'], os.environ['Region']).client 
    destinationLexClient = boto3.client('lexv2-models')
    sourceDynamoClient = Tydirium('dynamodb', ts, os.environ['AssumeRole'], os.environ['Region']).client 
    destinationDynamoClient = boto3.client('dynamodb') 
  else:
    sourceConnectClient = boto3.client('connect') 
    destinationConnectClient = sourceConnectClient 
    sourceLambdaClient = boto3.client('lambda') 
    destinationLambdaClient = sourceLambdaClient
    sourceLexClient = boto3.client('lexv2-models') 
    destinationLexClient = sourceLexClient
    sourceDynamoClient = boto3.client('dynamodb') 
    destinationDynamoClient = sourceDynamoClient
  return sourceConnectClient, destinationConnectClient, sourceLambdaClient, destinationLambdaClient, sourceLexClient, destinationLexClient, sourceDynamoClient, destinationDynamoClient   
  
