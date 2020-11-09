#Importing all the libraries
import sys
import os
import time
import boto3
from Libraries import utils

#To get all the stored variables from previous notebook
%store -r

#Creating a connection to AWS Forecast Service
session = boto3.Session(region_name='eu-west-1') 
forecast = session.client(service_name='forecast') 
forecastquery = session.client(service_name='forecastquery')

#Creating a Predictor using Dataset group arn

predictorName= project+'_deeparp_algo' #project varaible we defined in earlier notebook
forecastHorizon = 28 #number of days you need predictions
algorithmArn = 'arn:aws:forecast:::algorithm/Deep_AR_Plus' #You can take any algorithm arn. here we took deep ar plus

create_predictor_response_exp_4=forecast.create_predictor(PredictorName=predictorName, 
                                                  AlgorithmArn=algorithmArn,
                                                  ForecastHorizon=forecastHorizon,
                                                  PerformAutoML= False,
                                                  PerformHPO=False,
                                                  EvaluationParameters= {"NumberOfBacktestWindows": 3, 
                                                                         "BackTestWindowOffset": 28}, 
                                                  InputDataConfig= {"DatasetGroupArn": dataset_group_arn_exp_4},
                                                  FeaturizationConfig= {"ForecastDimensions": [ "Store_id"], #add the other dimensions apart from the product_id
                                                                        "ForecastFrequency": "D", 
                                                                        "Featurizations": 
                                                                        [
                                                                          {"AttributeName": "Sold Quantity", 
                                                                           "FeaturizationPipeline": 
                                                                            [
                                                                              {"FeaturizationMethodName": "filling", 
                                                                               "FeaturizationMethodParameters": #choose the missing values filling as per the use case
                                                                                {"aggregation": "sum",
                                                                                "frontfill": "none", 
                                                                                 "middlefill": "zero", 
                                                                                 "backfill": "zero"}
                                                                              }
                                                                            ]
                                                                          }
                                                                        ]
                                                                       }
                                                 )

#storing the predictor arn value
predictor_arn_exp_4=create_predictor_response_exp_4['PredictorArn']

#Checking the Status of Predictor creation
status_indicator = utils.StatusIndicator()

while True:
    status = forecast.describe_predictor(PredictorArn=predictor_arn_exp_4)['Status']
    status_indicator.update(status)
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)

status_indicator.end()

#Checkign the accuracy metrics of the predictor after the predictor creation
forecast.get_accuracy_metrics(PredictorArn=predictor_arn_exp_4)

#Create a Forecast using predictor arn

forecastName= project+'_deeparp_algo_forecast_exp'

#Creating the forecast
create_forecast_response=forecast.create_forecast(ForecastName=forecastName,
                                                  PredictorArn=predictor_arn_exp_4)

#Stroring the forecast arn                                                  
forecast_arn_exp_4 = create_forecast_response['ForecastArn']

#Checking the status of forecast arn creation
status_indicator = utils.StatusIndicator()

while True:
    status = forecast.describe_forecast(ForecastArn=forecast_arn_exp_4)['Status']
    status_indicator.update(status)
    if status in ('ACTIVE', 'CREATE_FAILED'): break
    time.sleep(10)

status_indicator.end()

#How to query the forecast arn for the products
forecastResponse = forecastquery.query_forecast(
    ForecastArn=forecast_arn,
    Filters={"Product_Id":"sku1","Store_id":"Store_1"}  #give the product id and store_id of your wish 
)
print(forecastResponse) #gives the predictions foe next 28 days