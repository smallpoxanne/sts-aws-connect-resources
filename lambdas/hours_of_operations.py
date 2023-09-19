import boto3
import logging
import os
import operator
# import s3_helper as S3

def sync_hours_of_operation(logger, ts, sourceConnectClient, destinationConnectClient, sourceArn, destinationArn):
  sourceId=sourceArn.split('/')[-1]
  destinationId=destinationArn.split('/')[-1]
  sourceHoursList = sourceConnectClient.list_hours_of_operations(
    InstanceId=sourceId
  )
  destinationHoursList = destinationConnectClient.list_hours_of_operations(
    InstanceId=destinationId
  )
  logger.debug(sourceHoursList['HoursOfOperationSummaryList'])
  logger.debug(destinationHoursList['HoursOfOperationSummaryList'])

  sourceHoursDescriptions = []
  destinationHoursDescriptions = []
  destinationHoursNames = {}

  for hours in sourceHoursList['HoursOfOperationSummaryList']:
    hoursDescription = sourceConnectClient.describe_hours_of_operation(
      InstanceId=sourceId,
      HoursOfOperationId=hours['Id']
      )
    sourceHoursDescriptions.append(hoursDescription['HoursOfOperation'])

  for hours in destinationHoursList['HoursOfOperationSummaryList']:
    destinationHoursNames[hours['Name']] = hours['Id']
    hoursDescription = destinationConnectClient.describe_hours_of_operation(
      InstanceId=destinationId,
      HoursOfOperationId=hours['Id']
      )
    destinationHoursDescriptions.append(hoursDescription['HoursOfOperation'])
    
  logger.info(f"sourceHoursDescriptions --- {sourceHoursDescriptions}")
  logger.info(f"destinationHoursDescriptions --- {destinationHoursDescriptions}")
  logger.info(f"destinationHoursNames --- {destinationHoursNames}")

  newHours = []
  updatedHours = []
  existingNotChangedHours = []
  notChangedUpdatedDueToError=[]
  for hoursDescription in sourceHoursDescriptions:
    hoursData = {
        "InstanceId":destinationId,
        "Name":hoursDescription['Name']
      }
    if hoursDescription['Description']:
      hoursData["Description"]=hoursDescription['Description']
    if hoursDescription['Tags']:
      hoursData["Tags"]=hoursDescription['Tags']
    if hoursDescription['TimeZone']:
      hoursData["TimeZone"]=hoursDescription['TimeZone']
    if hoursDescription['Config']:
      hoursData["Config"]=hoursDescription['Config']
      
    if(hoursDescription['Name'] in destinationHoursNames.keys()):
      destinationHoursList=[x for x in destinationHoursDescriptions if x['Name'] == hoursDescription['Name']]
      destinationHoursData=destinationHoursList[0]
      hoursData["HoursOfOperationId"]=""
      hoursData["HoursOfOperationId"]=destinationHoursNames[hoursDescription['Name']]
      srcConfigList = sorted(hoursDescription.get('Config'), key=operator.itemgetter('Day'))
      destConfigList = sorted(destinationHoursData.get('Config'), key=operator.itemgetter('Day'))
      if ((hoursDescription.get('Description')==destinationHoursData.get('Description')) or (not hoursDescription.get('Description') and not destinationHoursData.get('Description'))) \
        and hoursDescription.get('Tags')==destinationHoursData.get('Tags') \
        and hoursDescription.get('TimeZone')==destinationHoursData.get('TimeZone') \
        and srcConfigList==destConfigList:
        existingNotChangedHours.append(hoursDescription['Name'])
      else:
        try: 
          newHoursInfo = destinationConnectClient.update_hours_of_operation(**hoursData)
          updatedHours.append(hoursDescription['Name'])
        except Exception as error:
          logger.info(f"Exception -{hoursDescription['Name']}- not updated --- {type(error).__name__}")
          notChangedUpdatedDueToError.append(hoursDescription['Name'])
    else:
      try:
        newHoursInfo = destinationConnectClient.create_hours_of_operation(**hoursData)
        newHours.append(hoursDescription['Name'])
      except Exception as error:
          logger.info(f"Exception -{hoursDescription['Name']}- not created --- {type(error).__name__}")
          notChangedUpdatedDueToError.append(hoursDescription['Name'])

  logger.info(f"New Hours of Operation: {newHours}")
  logger.info(f"Updated Hours of Operation: {updatedHours}")
  logger.info(f"Existing Hours of Operation, not changed: {existingNotChangedHours}")
  logger.info(f"Hours of Operation, not changed/created due to error: {notChangedUpdatedDueToError}")