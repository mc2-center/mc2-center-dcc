import synapseclient
import json
from synapseclient import Folder, File

syn = synapseclient.login()

new_folder = Folder(name = 's3_index_testing', parent = None) #add parent folder/project Syn Id
new_folder = syn.store(new_folder)

BUCKET = None #add AWS bucket name here
KEY = None #insert file URI from AWS
FOLDER = new_folder.id

#make the S3 bucket visible to Synapse
destination = {'uploadType':'S3',
               'concreteType':'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting',
               'bucket':BUCKET}

destination = syn.restPOST('/storageLocation', body=json.dumps(destination))
print(destination['storageLocationId'])

#uncomment if you would like to use the bucket as the folder storage location
'''
project_destination ={'concreteType': 'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
                      'settingsType': 'upload'}
project_destination['locations'] = [] #[destination['storageLocationId']]
project_destination['projectId'] = FOLDER

project_destination = syn.restPOST('/projectSettings', body = json.dumps(project_destination))
'''

# create filehandle
fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
              'fileName'    : '', #insert file name
              'contentSize' : '', #insert file size in bytes
              'contentType' : '', #insert content type from AWS
              'contentMd5' :  '', #insert file Md5
              'bucketName' : BUCKET,
              'key' : '', #insert file URI from AWS
              'storageLocationId': destination['storageLocationId'] 
			  }

operation = '/externalFileHandle/s3'
fileHandle = syn.restPOST(operation, json.dumps(fileHandle), endpoint=syn.fileHandleEndpoint)
print(fileHandle)

#store fileHandle
f = File(parentId=FOLDER, dataFileHandleId = fileHandle['id'])

f = syn.store(f)
