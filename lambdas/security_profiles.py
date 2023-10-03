import logging
import boto3
from botocore.exceptions import ClientError
# import s3_helper as s3
import os
import time

def sync_security_profiles(logger, ts, sourceConnectClient, destinationConnectClient, sourceConnectArn, destinationConnectArn, resourceList):
  sourceId=sourceConnectArn.split('/')[-1]
  destinationId=destinationConnectArn.split('/')[-1]
  
  # get all sec profiles from source and destination
  sourceSpList = sourceConnectClient.list_security_profiles(
    InstanceId=sourceId
  )

  destinationSpList = destinationConnectClient.list_security_profiles(
    InstanceId=destinationId
  )

  # create lists of sec profile descriptions and names
  sourceSpDescriptions=[]
  destinationSpDescriptions=[]
  destinationSpNames=[]
  for ssp in sourceSpList['SecurityProfileSummaryList']:
    if ssp['Id'] in resourceList or resourceList[0]=="all" or resourceList[0]=="All":
      spDescResponse=sourceConnectClient.describe_security_profile(
        SecurityProfileId=ssp['Id'],
        InstanceId=sourceId
      )
      sourceSpDescriptions.append(spDescResponse['SecurityProfile'])
    
  for dsp in destinationSpList['SecurityProfileSummaryList']:
    spDescResponse=destinationConnectClient.describe_security_profile(
      SecurityProfileId=dsp['Id'],
      InstanceId=destinationId
    )
    destinationSpDescriptions.append(spDescResponse['SecurityProfile'])
    destinationSpNames.append(spDescResponse['SecurityProfile']['SecurityProfileName'])
    
  spCreated=[]
  spUpdated=[]
  spNotSynced=[]
  # loop through source descriptions for create/update
  for sspd in sourceSpDescriptions:
    spData={
      "InstanceId":destinationId,
    }
    if sspd.get('Description'):
      spData["Description"]=sspd['Description']
      secProfDescription=sspd['Description']
    if sspd.get('AllowedAccessControlTags'):
      spData["AllowedAccessControlTags"]=sspd['AllowedAccessControlTags']
      secProfAllowedAccessControlTags=sspd['AllowedAccessControlTags']
    if sspd.get('TagRestrictedResources'):
      spData["TagRestrictedResources"]=sspd['TagRestrictedResources']
      secProfTagRestrictedResources=sspd['TagRestrictedResources']
    if sspd.get('Tags'):
      spData["Tags"]=sspd['Tags']
      secProfTags=sspd['Tags']
      
    # make separate call to get list of sec profile permissions
    spPermsResponse = sourceConnectClient.list_security_profile_permissions(
      SecurityProfileId=sspd['Id'],
      InstanceId=sourceId,
      MaxResults=123
    )
    
    # if there are permissions in list, set permissions param of update/create object
    if len(spPermsResponse['Permissions'])>0:
      spData["Permissions"]=spPermsResponse['Permissions']
      secProfPermissions=spPermsResponse['Permissions']
    
    # if sec profile exists in destination, update
    if sspd['SecurityProfileName'] in destinationSpNames:
      # get destination sec prof description from list
      destSpDesc = next((x for x in destinationSpDescriptions if x['SecurityProfileName'] == sspd['SecurityProfileName']), None)
      
      ## when including: 
          # AllowedAccessControlTags=secProfAllowedAccessControlTags,
          # TagRestrictedResources=secProfTagRestrictedResources
          # call throws error
      try:
        updateSpResponse = destinationConnectClient.update_security_profile(
          Description=secProfDescription,
          Permissions=secProfPermissions,
          SecurityProfileId=destSpDesc['Id'],
          InstanceId=destinationId
          # AllowedAccessControlTags=secProfAllowedAccessControlTags,
          # TagRestrictedResources=secProfTagRestrictedResources
        )
        spUpdated.append(sspd['SecurityProfileName'])
      except Exception as error:
        logger.info(f"Exception -{sspd['SecurityProfileName']}- not updated due to error --- {type(error).__name__}")
        spNotSynced.append(sspd['SecurityProfileName'])
    
    # if sec profile doesn't exist in destination, create
    else:
      try:
        createSpResponse = destinationConnectClient.create_security_profile(
            InstanceId=destinationId,
            SecurityProfileName=sspd['SecurityProfileName'],
            Permissions=secProfPermissions,
            Tags=secProfTags,
            TagRestrictedResources=secProfTagRestrictedResources,
            AllowedAccessControlTags=secProfAllowedAccessControlTags
          )
        spCreated.append(sspd['SecurityProfileName'])
      except Exception as error:
        logger.info(f"Exception -{sspd['SecurityProfileName']}- not created due to error --- {type(error).__name__}")
        spNotSynced.append(sspd['SecurityProfileName'])
    
  logger.info(f"spCreated --- {spCreated}")
  logger.info(f"spUpdated --- {spUpdated}")
  logger.info(f"spNotSynced --- {spNotSynced}")