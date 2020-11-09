#Import all the required libraries
import pandas as pd
import boto3
import os
import time
from Libraries import utils

#Creating a connection to AWS Forecast Service
session = boto3.Session(region_name='eu-west-1') #Change the region name to your region
forecast = session.client(service_name='forecast') #Calling Forecast Service


#Intializing all the project variables
project = 'Demand_prediction' #change the project as per the use case
datasetGroupName= project +'_dsg'  #Datasetgroupname
bucket_name="" #Name of the s3 bucket where you stored the data

#Datasets folder paths in bucket
ts_key='notebook/Experiment4/Target_Timeseries_data.csv' #path to time series data (Change accordingly)
rts_key='notebook/Experiment4/Related_data.csv' #path to related_timeseries data (Change accordingly)
meta_key='notebook/Experiment4/Item_metadata.csv' #path to Metadata (Change accordingly)

#S3 datasets path
ts_s3DataPath = "s3://"+bucket_name+"/"+ts_key 
rts_s3DataPath = "s3://"+bucket_name+"/"+rts_key
meta_s3DataPath = "s3://"+bucket_name+"/"+meta_key
%store project # to store the varaibles to use for next jupyter notbook

#Dataset variables
DATASET_FREQUENCY = "D" 
TIMESTAMP_FORMAT = "yyyy-MM-dd"

#Creating a Dataset group to store all the datasets
dataset_arns = [] #to store the datasets
create_dataset_group_response_exp_4 = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                              DatasetArns=dataset_arns,
                                                              Domain="CUSTOM")
#Storing the datasetgroup arn to the variable                                                              
dataset_group_arn_exp_4 = create_dataset_group_response_exp_4['DatasetGroupArn']

#describe_dataset_group will give the properties(If Status is active means the dataset group created)
forecast.describe_dataset_group(DatasetGroupArn=dataset_group_arn_exp_4)    

#Add Datasets to the Dataset group

#Intializing the names of the datasets
ts_datasetName= project+'_ts_ds'
meta_datasetName=project+'_meta_ds'
rts_datasetName= project+'_rts_ds' 

#Creating the target time series data schema
target_schema ={
   "Attributes":[
      {
         "AttributeName":"Date",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"Product_Id",
         "AttributeType":"string"
      },
    {
         "AttributeName":"Store_id",
         "AttributeType":"string"
      },
       {
         "AttributeName":"Sold Quantity",
         "AttributeType":"float"
      }
   ]
}

#Item Metadata Schema
meta_schema = {
    "Attributes":[
      {
         "AttributeName":"Product_Id",
         "AttributeType":"string"
      },
    {
         "AttributeName":"Volume",
         "AttributeType":"string"
      },
       {
         "AttributeName":"Sub_Category",
         "AttributeType":"string"
      }
   ]
}                                                         

#Related TimeSeries Schema
target_schema ={
   "Attributes":[
      {
         "AttributeName":"Date",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"Product_Id",
         "AttributeType":"string"
      },
    {
         "AttributeName":"Store_id",
         "AttributeType":"string"
      },
       {
         "AttributeName":"Sold Price",
         "AttributeType":"float"
      }
   ]
}
                                                              
#Create Time Series Dataset
dataset_response_exp_4_ts = forecast.create_dataset(Domain="CUSTOM",
                               DatasetType='TARGET_TIME_SERIES',
                               DatasetName=ts_datasetName,
                               DataFrequency=DATASET_FREQUENCY,
                               Schema=target_schema
                              )

#Store Timeseries dataset arn
ts_dataset_arn_exp_4=dataset_response_exp_4_ts['DatasetArn']
#It will give the status of the arn if it got created or not
forecast.describe_dataset(DatasetArn=ts_dataset_arn_exp_4) 
 
#Create Item Metadata Dataset
dataset_response_exp_4_meta = forecast.create_dataset(Domain="CUSTOM",
                               DatasetType='ITEM_METADATA',
                               DatasetName=meta_datasetName,
                               DataFrequency=DATASET_FREQUENCY,
                               Schema=meta_schema
                              ) 

##Store Metadata dataset arn and describe_dataset will give status of dataset creation
meta_dataset_arn_exp_4=dataset_response_exp_4_meta['DatasetArn']
forecast.describe_dataset(DatasetArn=meta_dataset_arn_exp_4)

#Create Related Time Series dataset
dataset_response_exp_2_rts = forecast.create_dataset(Domain="CUSTOM",
                               DatasetType='RELATED_TIME_SERIES',
                               DatasetName=rts_datasetName,
                               DataFrequency=DATASET_FREQUENCY,
                               Schema=related_schema
                              )

#Store Related timeseries dataset arn and describe_dataset will give status of dataset creation
rts_dataset_arn_exp_2=dataset_response_exp_2_rts['DatasetArn']
forecast.describe_dataset(DatasetArn=rts_dataset_arn_exp_2)                         

#Appending all the datasets to the list
dataset_arns_exp_4 = []
dataset_arns_exp_4.append(ts_dataset_arn_exp_4)
dataset_arns_exp_4.append(meta_dataset_arn_exp_4)
dataset_arns_exp_4.append(rts_dataset_arn_exp_2)

#Add taget,metadata and related datasets to dataset group
forecast.update_dataset_group(DatasetGroupArn=dataset_group_arn_exp_4, DatasetArns=dataset_arns_exp_4)

#Import data to datasets
# Creatinf Time series import job
ts_data_import_job_name_exp_4='ts_data_import_job_exp_4'
ts_import_job_response_exp_3=forecast.create_dataset_import_job(DatasetImportJobName=ts_data_import_job_name_exp_4,
                                                          DatasetArn=ts_dataset_arn_exp_4,
                                                          DataSource= {
                                                              "S3Config" : {
                                                                 "Path":ts_s3DataPath,
                                                                 "RoleArn": '' #keep the role arn
                                                              } 
                                                          },
                                                          TimestampFormat=TIMESTAMP_FORMAT
                                                         )
#store dataset job arn
ts_import_job_arn_exp_4=ts_import_job_response_exp_3['DatasetImportJobArn']

#Below code snipped check the status of data import every 10 seconds and if the import is complete or failed than it will stop running
status_indicator = utils.StatusIndicator()
while True:
    status = forecast.describe_dataset_import_job(DatasetImportJobArn=ts_import_job_arn_exp_4)['Status']
    status_indicator.update(status)
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)

status_indicator.end() 


#Import Item Metadata
meta_data_import_job_name_exp_4='meta_data_import_job_exp_4'
meta_import_job_response_exp_4=forecast.create_dataset_import_job(DatasetImportJobName=meta_data_import_job_name_exp_4,
                                                          DatasetArn=meta_dataset_arn_exp_4,
                                                          DataSource= {
                                                              "S3Config" : {
                                                                 "Path":meta_s3DataPath,
                                                                 "RoleArn": '' # update the role arn
                                                              } 
                                                          },
                                                          TimestampFormat=TIMESTAMP_FORMAT
                                                         )

meta_import_job_arn_exp_4=meta_import_job_response_exp_4['DatasetImportJobArn']

#status of Metadata import job
status_indicator = utils.StatusIndicator()
while True:
    status = forecast.describe_dataset_import_job(DatasetImportJobArn=meta_import_job_arn_exp_4)['Status']
    status_indicator.update(status)
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)

status_indicator.end()         

#Import job for Related TIme series
rts_data_import_job_name_exp_2='rts_data_import_job_exp_2_latest'
rts_import_job_response_exp_2=forecast.create_dataset_import_job(DatasetImportJobName=rts_data_import_job_name_exp_2,
                                                          DatasetArn=rts_dataset_arn_exp_2,
                                                          DataSource= {
                                                              "S3Config" : {
                                                                 "Path":rts_s3DataPath,
                                                                 "RoleArn": '' #update the role arn
                                                              } 
                                                          },
                                                          TimestampFormat=TIMESTAMP_FORMAT
                                                         )
 
rts_import_job_arn_exp_2=rts_import_job_response_exp_2['DatasetImportJobArn'] 

#Status of Related time series data import job
status_indicator = utils.StatusIndicator()
while True:
    status = forecast.describe_dataset_import_job(DatasetImportJobArn=rts_import_job_arn_exp_2)['Status']
    status_indicator.update(status)
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)

status_indicator.end()

#Store Important variables for next jupyter notebook
%store dataset_group_arn_exp_4
%store 
%store role_arn
%store bucket_name
%store ts_s3DataPath
%store rts_s3DataPath
%store meta_s3DataPath
%store dataset_arns_exp_4
%store ts_dataset_arn_exp_4
%store meta_dataset_arn_exp_4
%store rts_dataset_arn_exp_2