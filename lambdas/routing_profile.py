import boto3
import logging
import os
import math
# import s3_helper as S3
import re

def sync_routing_profiles(logger, ts, lowerConnectClient, higherConnectClient, sourceConnectArn, destinationConnectArn, resourceList):
  rpsUpdated=[]
  rpsCreated=[]
  rpsNotSynced=[]
  # get lists of routing profiles from upper and lower envs
  lowerRoutingProfileList = lowerConnectClient.list_routing_profiles(
    InstanceId=sourceConnectArn,
    MaxResults=500
  )
  higherRoutingProfileList = higherConnectClient.list_routing_profiles(
    InstanceId=destinationConnectArn,
    MaxResults=500
  )
  lowerRpDescriptions = []
  higherRpDescriptions = []

  # make lists of rp descriptions for upper and lower envs
  for profile in lowerRoutingProfileList['RoutingProfileSummaryList']:
    if profile['Id'] in resourceList or resourceList[0]=="all" or resourceList[0]=="All":
      routingProfileDescription = lowerConnectClient.describe_routing_profile(
        InstanceId=sourceConnectArn,
        RoutingProfileId=profile['Id']
      )
      lowerRpDescriptions.append(routingProfileDescription['RoutingProfile'])
  for profile in higherRoutingProfileList['RoutingProfileSummaryList']:
    routingProfileDescription = higherConnectClient.describe_routing_profile(
      InstanceId=destinationConnectArn, 
      RoutingProfileId=profile['Id']
    )
    higherRpDescriptions.append(routingProfileDescription['RoutingProfile'])
    
  # We need to convert the Outbount Queue IDs from the lower environment IDs to their respective higher environment IDs
  # Creating a dictionary for quick translation
  queueIdTransformDict = get_queue_transforms(logger, ts, lowerConnectClient, higherConnectClient)
  lowerRpDescriptionsActive = [ x for x in lowerRpDescriptions if x['Name'][:2].lower() != "zz" and x['Name'][:2].lower() != "Zz" and x['Name'][:2].lower() != "zZ"] 
  for lrpd in lowerRpDescriptionsActive:
    # set variables that don't need to be transformed (Name,Description,MediaConcurrencies,Tags)
    lrpdName = lrpd['Name']
    lrpdDescription = lrpd['Description']
    lrpdMediaConcurrencies = lrpd['MediaConcurrencies']
    lrpdTags = lrpd['Tags']
    # get default outbound queue id from lower env, check if it exists in higher env, if not throw error, if it exists,
    # transform queue id and set hrpdDefaultOutboundQueueId
    lrpdDefaultOutboundQueueId = lrpd['DefaultOutboundQueueId']
    hrpdDefaultOutboundQueueId=""
    lrpdDefaultOutboundQueueName=""
    for k,v in queueIdTransformDict.items():
      if v['lowerId']==lrpdDefaultOutboundQueueId:
        lrpdDefaultOutboundQueueName = k
        if v['higherId']:
          hrpdDefaultOutboundQueueId = v['higherId']
    if hrpdDefaultOutboundQueueId=="":
      logger.info(f"***{lrpdName}*** routing profile not synced because default queue ***{lrpdDefaultOutboundQueueName}*** does not exist in higher env") 
      rpsNotSynced.append(lrpdName)
      continue
    # get lower queues associated with routing profile
    lrpdQueuesResponse = lowerConnectClient.list_routing_profile_queues( 
      InstanceId=sourceConnectArn,
      RoutingProfileId=lrpd['RoutingProfileId'],
      MaxResults=100
    )
    lrpdQueues = lrpdQueuesResponse['RoutingProfileQueueConfigSummaryList']
    
    # check if routing profile exists in higher env
    existingHigherRp = {}
    for hrpd in higherRpDescriptions:
      if hrpd['Name']==lrpdName:
        existingHigherRp=hrpd
        break
    # if routing profile does not exist, create rp
    if not existingHigherRp:
      # transform list of queues to queue configs variable
      queueConfigTransformDict = get_queues_lower_not_higher(lrpdQueues, queueIdTransformDict, logger)
      # if queuesNotInHigher has entries, log that resource can't be synced and print list of queues not in higher env
      if len(queueConfigTransformDict['queuesNotInHigher'])>0:
        logger.info(f"***{lrpdName}*** routing profile not created because queues ***{queueConfigTransformDict['queuesNotInHigher']}*** do not exist in higher env") 
        rpsNotSynced.append(lrpdName)
      # if queuesNotInHigher is zero move on to creating routing profile
      else: 
        try:
          # check for  queue config count, if greater than 10, must create rp with 10, save the new rp id, and then call associate queues for the rest 
          iterVal = math.ceil(len(queueConfigTransformDict['queueConfigs'])/10)
          tempConfigList=[]
          createdRpId = ""
          createResponse = higherConnectClient.create_routing_profile( 
            InstanceId=destinationConnectArn,
            Name=lrpdName,
            Description=lrpdDescription,
            DefaultOutboundQueueId=hrpdDefaultOutboundQueueId,
            QueueConfigs=tempConfigList,
            MediaConcurrencies=lrpdMediaConcurrencies,
            Tags=lrpdTags
          )
          logger.info(f"createResponse --- {createResponse}")
          createdRpId=createResponse['RoutingProfileId']
          rpsCreated.append(lrpdName)
          for i in range(0,iterVal):
            tempConfigList=queueConfigTransformDict['queueConfigs'][(i*10):((i*10)+10)]
            associateQueuesResponse = higherConnectClient.associate_routing_profile_queues(
              InstanceId=destinationConnectArn,
              RoutingProfileId=createdRpId,
              QueueConfigs=tempConfigList
            )
            logger.info(f"associateQueuesResponse --- {associateQueuesResponse}")
        except Exception as error:
          logger.info(f"***{lrpdName}*** routing profile not created because create request failed --- {type(error).__name__}") 
          rpsNotSynced.append(lrpdName)
    # if rp does exist in higher env, sync queues and update if possible
    else:
      #get queues associated with higher routing profile
      hrpdQueuesResponse = higherConnectClient.list_routing_profile_queues( 
        InstanceId=destinationConnectArn,
        RoutingProfileId=existingHigherRp['RoutingProfileId'],
        MaxResults=100
      )
      hrpdQueues = hrpdQueuesResponse['RoutingProfileQueueConfigSummaryList']
      # compare queues for lower rp and corresponding higher rp, function returns queueconfig var for matching queues,
      # queues in lower that dopn't exist in upper, and queues that need to be disassociated, which means either the queue
      # exists in the upper and not the lower, or the queue exists both places but is no longer associated with the lower rp 
      compareQueuesTransformDict = compare_existing_queue_configs(logger, lrpdQueues, hrpdQueues, queueIdTransformDict)
      # if queuesNotInHigher has entries, log that resource can't be synced and print list of queues not in higher env 
      if len(compareQueuesTransformDict['queuesNotInHigher'])>0:
        logger.info(f"***{lrpdName}*** routing profile not updated because queues ***{compareQueuesTransformDict['queuesNotInHigher']}*** do not exist in higher env")
        rpsNotSynced.append(lrpdName)
      # if queuesNotInHigher is zero move on to updating routing profile
      else:
        # update routing profile
        # check for differences before running individual update calls
        # update_routing_profile_concurrency
        
        if existingHigherRp['MediaConcurrencies']!=lrpd['MediaConcurrencies']:
          try:
            updateRpConcurrencyResponse = higherConnectClient.update_routing_profile_concurrency(
              InstanceId=destinationConnectArn,
              RoutingProfileId=existingHigherRp['RoutingProfileId'],
              MediaConcurrencies=lrpd['MediaConcurrencies']
            )
            logger.info(f"updateRpConcurrencyResponse --- {updateRpConcurrencyResponse}")
          except Exception as error:
            logger.info(f"***{lrpdName}*** concurrency not updated because update request failed --- {type(error).__name__}")
            # should I break here and move onto next routing profile if one thing doesn't update? 
            # rpsNotSynced.append(lrpdName)
        else:
          logger.info("not updating concurrency")
          
        # update_routing_profile_default_outbound_queue
        # get both default outbound queues, compare them in dictionary and only update if they are different
        
        currentHigherDefaultOutboundQueueId = existingHigherRp['DefaultOutboundQueueId']
        currentHigherDefaultOutboundQueueName=""
        logger.info(f"lrpdDefaultOutboundQueueName --- {lrpdDefaultOutboundQueueName}")
        for k,v in queueIdTransformDict.items():
          if v['higherId'] == currentHigherDefaultOutboundQueueId:
            currentHigherDefaultOutboundQueueName=k 
        logger.info(f"currentHigherDefaultOutboundQueueName --- {currentHigherDefaultOutboundQueueName}")
        if lrpdDefaultOutboundQueueName!=currentHigherDefaultOutboundQueueName:
          try:
            updateDefaultOutboundQueueResponse = higherConnectClient.update_routing_profile_default_outbound_queue( 
              InstanceId=destinationConnectArn, 
              RoutingProfileId=existingHigherRp['RoutingProfileId'],
              DefaultOutboundQueueId=hrpdDefaultOutboundQueueId
            )
            logger.info(f"updateDefaultOutboundQueueResponse --- {updateDefaultOutboundQueueResponse}")
          except Exception as error:
            logger.info(f"***{lrpdName}*** default outbound queue not updated because update request failed --- {type(error).__name__}")
        else:
          logger.info("not updating default outbound queue")
          
        # no code to update_routing_profile_name, must be done manually
        
        # update_routing_profile_queues
        # disassociate_routing_profile_queues
        # first disassociate queues
        try:
          disassociateQueueReferenceList = get_disassociate_queue_reference_list(hrpdQueues) 
          iterVal = math.ceil(len(disassociateQueueReferenceList)/10)
          tempConfigList=[]
          for i in range(0,iterVal):
            tempConfigList=disassociateQueueReferenceList[(i*10):((i*10)+10)]
            disassociateQueuesResponse = higherConnectClient.disassociate_routing_profile_queues(
              InstanceId=destinationConnectArn, 
              RoutingProfileId=existingHigherRp['RoutingProfileId'], 
              QueueReferences=tempConfigList
            )
            logger.info(f"disassociateQueuesResponse --- {disassociateQueuesResponse}")
        except:
          logger.info(f"***{lrpdName}*** queues not disassociated from corresponding higher routing profile due to failed disassociate queues call")
        
        # associate queues
        try:
          iterVal = math.ceil(len(compareQueuesTransformDict['queueConfigs'])/10)
          tempConfigList=[]
          for i in range(0,iterVal):
            tempConfigList=compareQueuesTransformDict['queueConfigs'][(i*10):((i*10)+10)]
            associateQueuesResponse = higherConnectClient.associate_routing_profile_queues(
              InstanceId=destinationConnectArn,
              RoutingProfileId=existingHigherRp['RoutingProfileId'],
              QueueConfigs=tempConfigList
            )
            logger.info(f"associateQueuesResponse --- {associateQueuesResponse}")
        except:
          logger.info(f"***{lrpdName}*** queues not associated to corresponding higher routing profile due to failed associate queues call") 
          rpsUpdated.append(lrpdName)
          
  logger.info(f"rpsUpdated --- {rpsUpdated}")
  logger.info(f"rpsCreated --- {rpsCreated}")
  logger.info(f"rpsNotSynced --- {rpsNotSynced}")
  
def compare_existing_queue_configs(logger, lowerQueuesList, higherQueuesList, queueIdTransformDict):
  disassociatedQueues=[]
  disassociatedQueuesNames = []
  lqNames=[]
  compareQueuesTransformDict = {
    "queueConfigs":[],
    "queuesNotInHigher":[],
    "disassociatedQueues":[],
    "disassociatedQueuesNames":[]
  }
  lowerNotHigherTransformDict = get_queues_lower_not_higher(lowerQueuesList, queueIdTransformDict, logger)
  compareQueuesTransformDict['queueConfigs']=lowerNotHigherTransformDict['queueConfigs']
  compareQueuesTransformDict['queuesNotInHigher']=lowerNotHigherTransformDict['queuesNotInHigher']
  for lq in lowerQueuesList:
    lqNames.append(lq['QueueName'])
  for hq in higherQueuesList:
    if hq['QueueName'] not in lqNames and hq['QueueName']:
      disassociatedQueues.append({
          'QueueId': hq['QueueId'],
          'Channel': hq['Channel']
        })
      disassociatedQueuesNames.append(hq['QueueName'])
  compareQueuesTransformDict['disassociatedQueuesNames']=disassociatedQueuesNames
  compareQueuesTransformDict['disassociatedQueues']=disassociatedQueues
  return compareQueuesTransformDict

def get_queues_lower_not_higher(queuesList, queueIdTransformDict, logger):
  queueConfigs = []
  queuesNotInHigher = []
  queueConfigTransformDict = {
    "queueConfigs":[],
    "queuesNotInHigher":[]
  }
  # loop through passed in queues list from lower env
  for queue in queuesList:
    queueId = ""
    # for each queue, loop through and check dictionary to see if queue exists in higher env
    for queueName,v in queueIdTransformDict.items():
      # if lower id of queues tranform dict matches id of queue from queuesList and higher Id is not empty in queues transform dict, set queueId
      if v['lowerId']==queue['QueueId'] and v['higherId']:
        queueId = v['higherId']
    # if higher queue id var is empty, add queue to queues not in higher list, continue out of loop onto the next queue from lower env queue list
    if queueId=="":
      queuesNotInHigher.append(queue['QueueName'])
      continue
    # if queueId is not empty, set queue config object with the updated queueId and add to queueConfigs list
    else:
      queueData = {
        'QueueReference': {
            'QueueId': queueId,
            'Channel': queue['Channel']
        },
        'Priority': queue['Priority'],
        'Delay': queue['Delay']
      }
      queueConfigs.append(queueData)
  queueConfigTransformDict['queuesNotInHigher']=queuesNotInHigher
  queueConfigTransformDict['queueConfigs']=queueConfigs
  return queueConfigTransformDict

def get_queues_higher_not_lower(queuesList, queueIdTransformDict):
  queueConfigs = []
  queuesNotInLower = [] 
  queueConfigTransformDict = {
    "queueConfigs":[],
    "queuesNotInLower":[]
  }
  # loop through passed in queues list from higher env
  for queue in queuesList:
    queueId = ""
    # for each queue, loop through and check dictionary to see if queue exists in lower env 
    for queueName,v in queueIdTransformDict.items():
      # if higher id of queues tranform dict matches id of queue from queuesList and lower Id is not empty in queues transform dict, set queueId 
      if v['higherId']==queue['QueueId'] and v['lowerId']:
        queueId = v['lowerId']
    # if lower queue id var is empty, add queue to queues not in lower list, continue out of loop onto the next queue from higher env queue list 
    if queueId=="":
      queuesNotInLower.append(queue['QueueName']) 
      continue
  # if queueId is not empty, set queue config object with the updated queueId and add to queueConfigs list
  else:
    queueData = {
        'QueueReference': {
            'QueueId': queueId,
            'Channel': queue['Channel']
        },
        'Priority': queue['Priority'],
        'Delay': queue['Delay']
      }
    queueConfigs.append(queueData)
  queueConfigTransformDict['queuesNotInLower']=queuesNotInLower
  queueConfigTransformDict['queueConfigs']=queueConfigs
  return queueConfigTransformDict

def get_queue_transforms(logger, ts, lowerConnectClient, higherConnectClient): 
  queueIdTransformDict = {}
  higherQueueList=higherConnectClient.list_queues(
    InstanceId=destinationConnectArn,
    QueueTypes=[
      'STANDARD'
    ]
  )
  for queue in higherQueueList['QueueSummaryList']:
    if queue['Name'] not in queueIdTransformDict.keys():
      queueIdTransformDict[queue['Name']] = {
        "higherId":queue['Id'],
        "lowerId":""
      }

  lowerQueueList=lowerConnectClient.list_queues(
    InstanceId=sourceConnectArn,
    QueueTypes=[
        'STANDARD'
    ]
  )
  for queue in lowerQueueList['QueueSummaryList']: 
    if queue['Name'] not in queueIdTransformDict.keys(): 
      queueIdTransformDict[queue['Name']] = {
        "higherId":"",
        "lowerId":queue['Id']
      }
    elif queue['Name'] in queueIdTransformDict.keys():
      queueIdTransformDict[queue['Name']]['lowerId']=queue['Id']
  return queueIdTransformDict
  
def get_disassociate_queue_reference_list(queuesList):
  queueConfigs = []
  for q in queuesList:
    queueData = {
        'QueueId': q['QueueId'],
        'Channel': q['Channel']
    }
    queueConfigs.append(queueData)
  return queueConfigs