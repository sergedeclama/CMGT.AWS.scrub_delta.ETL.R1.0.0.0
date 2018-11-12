from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import datetime
from datetime import date
from datetime import timedelta
import sys  # required for fetching command line arguments
import os   # required for calling c
from boto3.dynamodb.conditions import Key, Attr
from datetime import timedelta  

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    # TODO implement

    try:
        
        #--{Get Function Event Arguments}    
        job_properties = json.loads(json.dumps(event))

        #--{Check to see if clusterName exist}
        if "clusterName" not in job_properties:
            return {
                "statusCode": 400,
                "statusDescription": "Bad Request",
                "body": "Cluster Not Created"
            }
            
        _currentProcessDate = date.today()-timedelta(days=int(job_properties["currentDays"]))

        _currentProcessDateSplit = str(_currentProcessDate).split('-')
        _previousProcessDate = date.today()-timedelta(days=int(job_properties["previousDays"]))
        _previousProcessDateSplit = str(_previousProcessDate).split('-')
        if len(_currentProcessDateSplit)==3:
            currentPartitionYear=_currentProcessDateSplit[0]
            currentPartitionMonth=_currentProcessDateSplit[1]
            currentPartitionDay=_currentProcessDateSplit[2]

        if len(_previousProcessDateSplit)==3:
            previousPartitionYear=_previousProcessDateSplit[0]
            previousPartitionMonth=_previousProcessDateSplit[1]
            previousPartitionDay=_previousProcessDateSplit[2]

        #--{Create DynamoDB Object}
        dynamodbClient = boto3.resource('dynamodb')
        
        #--{Create DynamoDB Table Objects}
        clusterTable = dynamodbClient.Table('emr_cluster_properties')
        propertyTable = dynamodbClient.Table('emr_delta_properties')
    
        #--{Query DynamoDB Table for EMR Cluster Properties}
        clusterResponse =  clusterTable.scan(FilterExpression=Attr('Name').eq(job_properties["clusterName"]))

        propertiesDataSet = []
        
        response = propertyTable.scan(FilterExpression=Attr('sparkFileName').eq(job_properties['workflowname']) & Attr('sparkBusinessProperty').eq(job_properties['businessProperty']))
        for i in response['Items']:
            propertiesDataSet.append(i)

        while 'LastEvaluatedKey' in response:
            response = propertyTable.scan(FilterExpression=Attr('sparkFileName').eq(job_properties['workflowname']) & Attr('sparkBusinessProperty').eq(job_properties['businessProperty']), ExclusiveStartKey=response['LastEvaluatedKey'])
            for i in response['Items']:
                print(json.dumps(i, cls=DecimalEncoder))
                propertiesDataSet.append(i)
        
    
        #--{Set DynamoDB DataSet}
        clusterDataSet = clusterResponse['Items']
        #propertiesDataSet = propertyResponse['Items']
        
        #--{Create Cluster Properties JSON Objects}
        emrClusterJSON = json.loads(json.dumps(clusterDataSet, cls=DecimalEncoder))
        emrStepsJSON = json.loads(json.dumps(propertiesDataSet, cls=DecimalEncoder))

        # print (json.dumps(emrClusterJSON[0], cls=DecimalEncoder))
        # print (json.dumps(emrStepsJSON[0], cls=DecimalEncoder))
        #return

        #--{Check to see if ID exists}
        if "id" not in emrClusterJSON[0]:
            return {
                "statusCode": 404,
                "statusDescription": "Not Found",
                "body": "Cluster Not Created"
            }

        #--{Check to see if ID exists}
        if "id" not in emrStepsJSON[0]:
            return {
                "statusCode": 404,
                "statusDescription": "Not Found",
                "body": "Cluster Not Created"
            }

        #--{Create Empty Hadoop Steps Dict}
        hadoop_steps = []
        
        #--{Loop Through Each EMR Steps}
        for step in emrStepsJSON:
            _spark_app_name = (step["sparkCommandLineArguments"]["sparkAppName"])
            _path_previous_raw_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkRawPreviousPath"])+previousPartitionYear+'/'+previousPartitionMonth+'/'+previousPartitionDay+'/'
            _path_current_raw_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkRawCurrentPath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/'
            _path_insert_deltas_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkDeltaDataPath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/inserts/'
            _path_delete_deltas_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkDeltaDataPath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/deletes/'
            _path_current_bad_records_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkBadRecordsPath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/'
            _path_audit_records_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkAuditRecordsPath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/'
            _path_config_json_file = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkConfigurationPath"])
            _filename = (step["sparkCommandLineArguments"]["sparkFileFormat"]).format(currentPartitionYear, currentPartitionMonth, currentPartitionDay)
            _sparkInputBucketFolder =  (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkInputBucketFolder"]).format(currentPartitionYear, currentPartitionMonth, currentPartitionDay)
            _delete_delta_path = (step["sparkCommandLineArguments"]["sparkDeltaDeletePath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/'
            _delete_bad_path = (step["sparkCommandLineArguments"]["sparkDeltaBadPath"])+currentPartitionYear+'/'+currentPartitionMonth+'/'+currentPartitionDay+'/'
            _sparkOutputBucketFolder = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkOutputBucket"])
            _sparkBadBucketFolder = (step["sparkCommandLineArguments"]["sparkBasePath"])+(step["sparkCommandLineArguments"]["sparkBadBucket"])

            #--{Set Hadoop Step Properties}
            hadoop_step = {"Name": step["appType"], "ActionOnFailure": step["actionOnFailure"], "HadoopJarStep": {"Jar": "command-runner.jar", 
            "Args": ["spark-submit", 
            "--deploy-mode", step["sparkDeployMode"],
            "--executor-memory", step["sparkExecutorMemory"],
            "--num-executors",   str(step["sparkNumberExecutors"]),
            "--executor-cores",  str(step["sparkExecutorCores"]),
            "--driver-memory",   step["sparkDriveMemory"],
            (step["sparkCommandLineArguments"]["sparkBasePath"]+step["sparkDeltaScript"]),
            (_spark_app_name),
            (_path_previous_raw_file),
            (_path_current_raw_file),
            (_path_insert_deltas_file),
            (_path_delete_deltas_file),
            (_path_current_bad_records_file),
            (_path_audit_records_file),
            (_path_config_json_file),
            (step["sparkCommandLineArguments"]["sparkDelimiterOutput"]),
            (step["sparkCommandLineArguments"]["sparkDelimiterSource"]),
            (step["sparkCommandLineArguments"]["sparkAdditionalOutput"]),
            (step["sparkCommandLineArguments"]["sparkLookupTableName"]),
            (step["sparkCommandLineArguments"]["sparkFlagReseed"]),
            (step["sparkCommandLineArguments"]["sparkFlagFormatData"]),
            (step["sparkCommandLineArguments"]["sparkFlagJSONStringFormatting"]),
            (step["sparkCommandLineArguments"]["sparkFlagValidateRecords"]),
            (step["sparkCommandLineArguments"]["sparkFlagRegexValidation"]),
            (step["sparkCommandLineArguments"]["sparkFlagReplaceData"]),
            (step["sparkCommandLineArguments"]["sparkRegexValidation"]),
            (step["sparkCommandLineArguments"]["sparkJSONReplacement"]),
            (step["sparkCommandLineArguments"]["sparkJSONFormating"]),
            (step["sparkCommandLineArguments"]["sparkJSONValidation"]),
            (step["sparkCommandLineArguments"]["sparkSourceType"]),
            (step["sparkCommandLineArguments"]["sparkCsvHasHeader"]),
            (currentPartitionYear),
            (currentPartitionMonth),
            (currentPartitionDay),
            (_filename),
            (_sparkInputBucketFolder),
            (step["sparkCommandLineArguments"]["sparkBucketDeleteBeforeWrite"]),
            (_delete_delta_path),
            (_delete_bad_path),
            (step["sparkCommandLineArguments"]["sparkOutputBucket"]),
            (step["sparkCommandLineArguments"]["sparkBadBucket"])
            ]}}     
            
            #--{Append Step to Step Dict}
            hadoop_steps.append(hadoop_step)

        #--{Add Steps to ClusterInfo}
        emrClusterJSON[0]['ClusterInfo']['Steps'] = json.loads(json.dumps([hadoop_steps]))[0]
        print (json.dumps(emrClusterJSON[0], cls=DecimalEncoder))
        #return

        #print (emrClusterJSON[0]['ClusterInfo']['Name'])
        #print (emrClusterJSON[0]['ClusterInfo']['LogUri'])
        #print (emrClusterJSON[0]['ClusterInfo']['ReleaseLabel'])
        #print (emrClusterJSON[0]['ClusterInfo']['Instances'])
        #print ('Steps=' + str(emrClusterJSON[0]['ClusterInfo']['Steps']))
        #print ("Applications=" + json.dumps(emrClusterJSON[0]['ClusterInfo']['Applications']))
        #print (emrClusterJSON[0]['ClusterInfo']['VisibleToAllUsers'])
        #print (emrClusterJSON[0]['ClusterInfo']['ServiceRole'])
        #print (emrClusterJSON[0]['ClusterInfo']['JobFlowRole'])
        #print (emrClusterJSON[0]['ClusterInfo']['Tags'])
        #print (emrClusterJSON[0]['ClusterInfo']['AutoScalingRole'])
        #print (emrClusterJSON[0]['ClusterInfo']['ScaleDownBehavior'])
       
        #return (emrClusterJSON[0]['ClusterInfo'])
        emrClient = boto3.client('emr')
        
        #--{Parase EMR Property JSON to create an EMR Cluster}
        response = emrClient.run_job_flow(
            Name=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['Name']))+'['+currentPartitionYear+'.'+currentPartitionMonth+'.'+currentPartitionDay+']',
            LogUri=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['LogUri'])),
            ReleaseLabel=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['ReleaseLabel'])),
            Instances=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['Instances'])),
        	Steps=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['Steps'])),
            Applications=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['Applications'])),
            VisibleToAllUsers=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['VisibleToAllUsers'])),
            ServiceRole=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['ServiceRole'])),
            JobFlowRole=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['JobFlowRole'])),
            Tags=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['Tags'])),
            AutoScalingRole=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['AutoScalingRole'])),
            ScaleDownBehavior=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['ScaleDownBehavior'])),
            EbsRootVolumeSize=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['EbsRootVolumeSize'])),
            BootstrapActions=json.loads(json.dumps(emrClusterJSON[0]['ClusterInfo']['BootstrapActions']))
    	    )
        return {
            "statusCode": 200,
            "statusDescription": "OK",
            "body": "Cluster Created"
        }
    
    except Exception as ers:
        return {
            "statusCode": 400,
            "statusDescription": "Bad Request",
            "body": ers
        }

