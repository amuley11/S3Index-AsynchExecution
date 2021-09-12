import json
import boto3

ddb = boto3.resource('dynamodb')
s3metadata = ddb.Table('s3metadata')
item_count = 0

def f_S3Indexing(event, context):
    '''
    A function created for indexing an object added/modified/deleted from the S3 bucket into a DynamoDB table
    '''
    for i in range(0,len(event['Records'])):
        S3BucketRegion = event['Records'][0]['awsRegion']
        S3BucketName = event['Records'][0]['s3']['bucket']['name']
        S3ObjectKey = event['Records'][0]['s3']['object']['key']
        S3ObjectSize = event['Records'][0]['s3']['object']['size']
        S3ObjecteTag = event['Records'][0]['s3']['object']['eTag']
        EventTime = event['Records'][0]['eventTime']
        UserID = event['Records'][0]['userIdentity']['principalId']
        SourceIP = event['Records'][0]['requestParameters']['sourceIPAddress']
        try:
            S3ObjectVersion = event['Records'][0]['s3']['object']['versionId']
        except:
            S3ObjectVersion = 'Not Applicable'
        
        s3metadata.put_item(Item={'S3BucketRegion': S3BucketRegion, 'S3BucketName': S3BucketName, 'S3ObjectKey': S3ObjectKey, 
        'S3ObjectSize': S3ObjectSize, 'S3ObjectSize': S3ObjectSize, 'S3ObjectVersion': S3ObjectVersion,
        'S3ObjecteTag': S3ObjecteTag, 'EventTime': EventTime, 'UserID': UserID, 'SourceIP': SourceIP})
        
        print("Record for " + S3BucketName + " bucket's " + S3ObjectKey + " object added to the DynamoDB table.")
        
        item_count += 1

    return "Process is successful. " + str(item_count) + " item(s) are added to the s3metadata DynamoDB table."
