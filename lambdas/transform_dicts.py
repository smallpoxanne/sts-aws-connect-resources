import boto3
import logging
import os
import math
# import s3_helper as S3
import re
import json
import sys

def get_phone_number_transform_dicts(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId):
    sourceNumbersList = sourceConnectClient.list_phone_numbers(
        InstanceId=sourceId
    )
    destinationNumbersList = destinationConnectClient.list_phone_numbers(
        InstanceId=destinationId
    )
    sourcePnDescriptions=[]
    destinationPnDescriptions=[]
    for pn in sourceNumbersList['PhoneNumberSummaryList']:
      pnDescribe = sourceConnectClient.describe_phone_number(
        PhoneNumberId=pn['Id']
      )
      sourcePnDescriptions.append(pnDescribe)
    for pn in destinationNumbersList['PhoneNumberSummaryList']:
      pnDescribe = destinationConnectClient.describe_phone_number(
        PhoneNumberId=pn['Id']
      )
      destinationPnDescriptions.append(pnDescribe)
    numberTransformDict = {}
    return numberTransformDict
    
def get_hours_transform_dict(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId):
    hoursTransformDict = {}
    sourceHoursList = sourceConnectClient.list_hours_of_operations(
        InstanceId=sourceId
    )
    destinationHoursList = destinationConnectClient.list_hours_of_operations(
        InstanceId=destinationId
    )
    for hours in sourceHoursList['HoursOfOperationSummaryList']:
      if hours['Name'] not in hoursTransformDict.keys():
        hoursTransformDict[hours['Name']] = {
          "sourceId":hours['Id'],
          "destinationId":""
        }
    for hours in destinationHoursList['HoursOfOperationSummaryList']:
      if hours['Name'] not in hoursTransformDict.keys(): 
        hoursTransformDict[hours['Name']] = {
          "destinationId":hours['Id'],
          "sourceId":""
        }
      elif hours['Name'] in hoursTransformDict.keys():
        hoursTransformDict[hours['Name']]['destinationId']=hours['Id']
    return hoursTransformDict
  
def get_quick_connects_transform_dict(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId):
    quickConnectsTransformDict = {}
    sourceQuickConnectsList = sourceConnectClient.list_quick_connects(
        InstanceId=sourceId
    )
    destinationQuickConnectsList = destinationConnectClient.list_quick_connects(
        InstanceId=destinationId
    )
    sourceQcNames=[]
    destQcNames=[]
    for qc in sourceQuickConnectsList['QuickConnectSummaryList']:
      sourceQcNames.append(qc['Name'])
      if qc['Name'] not in quickConnectsTransformDict.keys():
        quickConnectsTransformDict[qc['Name']] = {
          "sourceIdNumber":qc['Id'],
          "destinationIdNumber":""
        }
    for qc in destinationQuickConnectsList['QuickConnectSummaryList']:
      destQcNames.append(qc['Name'])
      if qc['Name'] not in quickConnectsTransformDict.keys(): 
        quickConnectsTransformDict[qc['Name']] = {
          "sourceIdNumber":"",
          "destinationIdNumber":qc['Id']
        }
      elif qc['Name'] in quickConnectsTransformDict.keys():
        quickConnectsTransformDict[qc['Name']]['destinationIdNumber']=qc['Id']
        
    qcNotInDestination = [x for x in sourceQcNames if x not in destQcNames]
    return quickConnectsTransformDict,qcNotInDestination
    
def get_phone_number_transform_dict_hardcoded(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId):
    pnTransformDict = {
        "00 Med Group Main Menu":{
            "sourceIdNumber":"d2c35307-41a2-499b-b425-2d4900ce0b9e",
            "destinationIdNumber":"50befa7e-22fe-46f9-8a77-abf5f3424bdd"
        }
    }
    return pnTransformDict