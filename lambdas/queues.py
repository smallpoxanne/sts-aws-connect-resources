import boto3
import logging
import os
import math
# import s3_helper as S3
import re
import json
import transform_dicts as TransformDicts

def sync_connect_queues(logger, ts, sourceConnectClient, destinationConnectClient, sourceArn, destinationArn, resourceList):
  sourceId=sourceArn.split('/')[-1]
  destinationId=destinationArn.split('/')[-1]
  # get lists of queues from source and destination
  sourceQueueList = sourceConnectClient.list_queues(
      InstanceId=sourceId, 
      QueueTypes=["STANDARD"],
  )
  destinationQueueList = destinationConnectClient.list_queues(
      InstanceId=destinationId,
      QueueTypes=["STANDARD"],
  )

  sourceQueueDescriptions = []
  destinationQueueDescriptions = []
  destinationQueueNames = {}
  
  # create lists of queue descriptions from source and destination
  for queue in sourceQueueList["QueueSummaryList"]:
    if queue['Id'] in resourceList or resourceList[0]=="all" or resourceList[0]=="All":
      queueDescription = sourceConnectClient.describe_queue(
          InstanceId=sourceId, 
          QueueId=queue["Id"],
      )
      sourceQueueDescriptions.append(queueDescription["Queue"])
  for queue in destinationQueueList["QueueSummaryList"]:
    destinationQueueNames[queue["Name"]] = queue["Id"]
    queueDescription = destinationConnectClient.describe_queue(
        InstanceId=destinationId,
        QueueId=queue["Id"],
    )
    destinationQueueDescriptions.append(queueDescription["Queue"])
      
  # get transform dictionaries for dependencies: phone numbers, hours, quick connects     
  phoneNumbersTransformDict,pnNotInDestination = TransformDicts.get_phone_number_transform_dict_hardcoded(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId)
  hoursTransformDict,hoursNotInDestination = TransformDicts.get_hours_transform_dict(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId)
  quickConnectsTransformDict,qcNotInDestination = TransformDicts.get_quick_connects_transform_dict(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId)
  contactFlowsTransformDict,cfNotInDestination = TransformDicts.get_contact_flow_transform_dict(logger, ts, sourceConnectClient, destinationConnectClient, sourceId, destinationId)
  
  queuesCreated=[]
  queuesUpdated=[]
  queuesNotSynced=[]
  for sqd in sourceQueueDescriptions:
    badOutboudFlow=False
    badQuickConnect=False
    badPhoneNumber=False
    # set variables that don't need to be transformed (Name,Description,Tags, max contacts)
    sourceQueueName = sqd['Name']
    sourceQueueDescription = sqd.get('Description')
    sourceQueueTags = sqd['Tags']
    sourceQueueMaxContacts = sqd.get('MaxContacts')
    # set hours of operation ID with transform dict
    sourceHoursId=sqd['HoursOfOperationId']
    sourceHoursName=next(iter({key for (key, value) in hoursTransformDict.items() if value['sourceIdNumber']==sourceHoursId}))
    # if hours don't exist in destination env, log error, continue
    if sourceHoursName in hoursNotInDestination:
      logger.info(f"***{sourceQueueName}*** queue not synced because hours of operation ***{sourceHoursName}*** does not exist in destination env") 
      queuesNotSynced.append(sourceHoursName)
      continue
    else:
      destHoursId=hoursTransformDict[sourceHoursName]["destinationIdNumber"]
    # get associated quick connects, set quick connects transforms with transform dictionary  
    destQuickConnectIds=[]
    queueQuickConnectsResponse = sourceConnectClient.list_queue_quick_connects(
      InstanceId=sourceId,
      QueueId=sqd['QueueId'],
      MaxResults=99
    )
    for sqc in queueQuickConnectsResponse['QuickConnectSummaryList']:
      sourceQuickConnectName=next(iter({key for (key, value) in quickConnectsTransformDict.items() if value['sourceIdNumber']==sqc['Id']}))
      # if quick connect not in dest env sync without 
      if sourceQuickConnectName in qcNotInDestination:
        logger.info(f"***{sourceQueueName}*** queue > quick connect ***{sourceQuickConnectName}***, does not exist in destination env, proceeding...")
        badQuickConnect=True
      else:
        destQuickConnectIds.append(quickConnectsTransformDict[sourceQuickConnectName]["destinationIdNumber"])
    
    # set OutboundCallerConfig dict
    destOutboundCallerConfig={}
    if sqd.get('OutboundCallerConfig'):
      
      # set outbound caller Id name, outbound caller ID name is always the same between envs, no transformation needed
      if sqd['OutboundCallerConfig'].get('OutboundCallerIdName'):
        destOutboundCallerConfig['OutboundCallerIdName']=sqd['OutboundCallerConfig']['OutboundCallerIdName']
        
      #transform phone number ID
      if sqd['OutboundCallerConfig'].get('OutboundCallerIdNumberId'):
        sourcePhoneNumberName=next(iter({key for (key, value) in phoneNumbersTransformDict.items() if value['sourceIdNumber']==sqd['OutboundCallerConfig']['OutboundCallerIdNumberId']}))
        if sourcePhoneNumberName in pnNotInDestination:
          logger.info(f"***{sourceQueueName}*** queue > outbound number, ***{sourcePhoneNumberName}***, does not exist in destination env, proceeding...")
          badPhoneNumber=True
        else:
          destOutboundCallerConfig['OutboundCallerIdNumberId']=phoneNumbersTransformDict[sourcePhoneNumberName]["destinationIdNumber"]
          
      #transform outbound contact flow id
      if sqd['OutboundCallerConfig'].get('OutboundFlowId'):
        sourceContactFlowName=next(iter({key for (key, value) in contactFlowsTransformDict.items() if value['sourceIdNumber']==sqd['OutboundCallerConfig']['OutboundFlowId']}))
        if sourceContactFlowName in cfNotInDestination:
          logger.info(f"***{sourceQueueName}*** queue > outbound contact flow, ***{sourceContactFlowName}***, does not exist in destination env, proceeding...")
          badOutboudFlow=True
        else:
          destOutboundCallerConfig['OutboundFlowId']=contactFlowsTransformDict[sourceContactFlowName]["destinationIdNumber"]
    
    # get source queue status
    sourceQueueStatus=sqd["Status"]
    
    # create or update queue after checking if it exists in destination env
    if sourceQueueName in destinationQueueNames.keys():
      queueUpdated = False
      destQueueDesc = next((x for x in destinationQueueDescriptions if x['Name'] == sourceQueueName), None)
      
      if destQueueDesc['Description']!=sourceQueueDescription:
        logger.info(f"***{sourceQueueName}*** queue > descriptions different, please update manually, proceeding...")
      if destQueueDesc['Tags']!=sourceQueueTags:
        logger.info(f"***{sourceQueueName}*** queue > tags different, please update manually, proceeding...")
      
      # get both hours of operation, compare, update if different (already chedk above if hours of operation exist in both envs)
      sourceHoursName=next(iter({key for (key, value) in hoursTransformDict.items() if value['sourceIdNumber']==sourceHoursId}))
      destHoursName=next(iter({key for (key, value) in hoursTransformDict.items() if value['destinationIdNumber']==destQueueDesc['HoursOfOperationId']}))
      if sourceHoursName!=destHoursName:
        try:
          updateHoursResponse = destinationConnectClient.update_queue_hours_of_operation(
            InstanceId=destinationId,
            QueueId=destQueueDesc['QueueId'],
            HoursOfOperationId=hoursTransformDict[sourceHoursName]['destinationIdNumber']
          )
          queueUpdated=True
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- hours not updated due to error --- {type(error).__name__}")
      
      # get both max contact values, compare, update if different 
      destMaxContacts=destQueueDesc.get('MaxContacts')
      if sourceQueueMaxContacts!=destMaxContacts:
        try:
          updateMaxContactsResponse = destinationConnectClient.update_queue_max_contacts(
            InstanceId=destinationId,
            QueueId=destQueueDesc['QueueId'],
            MaxContacts=sourceQueueMaxContacts
          )
          queueUpdated=True
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- max contacts not updated due to error --- {type(error).__name__}")
      
      # check for bad phone number or bad contact flow, if True, skip updating
      if badOutboudFlow:
        logger.info(f"***{sourceQueueName}*** queue > outbound contact flow does not exist in destination env, queue config not updated")
      elif badPhoneNumber:
        logger.info(f"***{sourceQueueName}*** queue > outbound phone number does not exist in destination env, queue config not updated")
      else:
        updateCallerConfigData={
          "InstanceId":destinationId,
          "QueueId":destQueueDesc['QueueId']
        }
        if destOutboundCallerConfig:
          updateCallerConfigData['OutboundCallerConfig']=destOutboundCallerConfig
        else:
          updateCallerConfigData['OutboundCallerConfig']={}
        
        try:
          updateQueueConfigResponse = destinationConnectClient.update_queue_outbound_caller_config(**updateCallerConfigData)
          queueUpdated=True
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- queue config not updated successfully due to error --- {type(error).__name__}")
      
      # check if quick connects are equal, if not, disassociate all qcs then associate tranformed list from above
      try:
        destQuickConnectsResponse = destinationConnectClient.list_queue_quick_connects(
          InstanceId=destinationId,
          QueueId=destQueueDesc['QueueId'],
          MaxResults=99
        ) 
      except Exception as error:
        logger.info(f"Exception -{sourceQueueName}- queue config not updated successfully due to error listing dest quick connects --- {type(error).__name__}")
        queuesNotSynced.append(sourceQueueName)
        continue
          
      existingDestQcIds=[]
      for destqc in destQuickConnectsResponse['QuickConnectSummaryList']:
        existingDestQcIds.append(destqc['Id'])
        
      listdiffs = list(set(destQuickConnectIds) - set(existingDestQcIds))
      listdiffs2 = list(set(existingDestQcIds) - set(destQuickConnectIds))
      if len(listdiffs)>0 or len(listdiffs2)>0:
        try:
          if len(existingDestQcIds)>0:
            disQcResponse = destinationConnectClient.disassociate_queue_quick_connects(
              InstanceId=destinationId,
              QueueId=destQueueDesc['QueueId'],
              QuickConnectIds=existingDestQcIds
            )
            queueUpdated=True
          if len(destQuickConnectIds)>0:
            assQcResponse = destinationConnectClient.associate_queue_quick_connects(
              InstanceId=destinationId,
              QueueId=destQueueDesc['QueueId'],
              QuickConnectIds=destQuickConnectIds
            )
            queueUpdated=True
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- quick connects not updated successfully due to error --- {type(error).__name__}")
      
      # get queue statuses, compare and update if not equal
      if destQueueDesc['Status']!=sourceQueueStatus:
        try:
          updateStatusResponse = destinationConnectClient.update_queue_status(
            InstanceId=destinationId,
            QueueId=destQueueDesc['QueueId'],
            Status=sourceQueueStatus
          )
          queueUpdated=True
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- status not updated due to error --- {type(error).__name__}")
      
      if queueUpdated:
        if badQuickConnect:
          logger.info(f"***{sourceQueueName}*** updated with incomplete quick connect list")
        queuesUpdated.append(sourceQueueName)
      else:
        queuesNotSynced.append(sourceQueueName)
    else:
      # set create/update queue object
      createQueueData = {
        "InstanceId":destinationId,
        "Name":sourceQueueName,
        "HoursOfOperationId":destHoursId,
      }
      if sourceQueueDescription:
        createQueueData['Description']=sourceQueueDescription
      if destOutboundCallerConfig:
        createQueueData['OutboundCallerConfig']=destOutboundCallerConfig
      if sourceQueueMaxContacts:
        createQueueData['MaxContacts']=sourceQueueMaxContacts
      if sourceQueueTags:
        createQueueData['Tags']=sourceQueueTags
      try:
        createQueueResponse=destinationConnectClient.create_queue(**createQueueData)
        queuesCreated.append(sourceQueueName)
        createdQueueId=createQueueResponse['QueueId']
      except Exception as error:
        logger.info(f"Exception -{sourceQueueName}- not created --- {type(error).__name__}")
        queuesNotSynced.append(sourceQueueName)
        continue
      if len(destQuickConnectIds)>0:
        # associate quick connects to created queue
        try:
          associateQcResponse = destinationConnectClient.associate_queue_quick_connects(
            InstanceId=destinationId,
            QueueId=createdQueueId,
            QuickConnectIds=destQuickConnectIds
          )
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- quick connects not associated due to error --- {type(error).__name__}")
      
      # all created queues are enabled by default, check source queue to see if it is disbaled, if yes disable created queue
      if sqd['Status']=='DISABLED':
        try:
          updateStatusResponse = destinationConnectClient.update_queue_status(
            InstanceId=destinationId,
            QueueId=createdQueueId,
            Status='DISABLED'
          )
        except Exception as error:
          logger.info(f"Exception -{sourceQueueName}- status not changed to DISABLED due to error --- {type(error).__name__}")
 
  logger.info(f"queuesCreated --- {queuesCreated}")
  logger.info(f"queuesUpdated --- {queuesUpdated}")
  logger.info(f"queuesNotSynced --- {queuesNotSynced}")
      
      
      
