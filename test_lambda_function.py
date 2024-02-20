import pytest
from lambda_function import getObjectFromBucket,objectToDict,lambda_handler
import boto3
from datetime import datetime
from moto import mock_s3, mock_ses
import os
import json

###====Before=======================
@pytest.fixture
def s3(aws_credentials):
    with moto.mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture
def obj_test(s3):
    s3.create_bucket(Bucket='mybucket')
    s3.put_object(Bucket='mybucket', Key='mykey', Body='mybody')
    response = s3.get_object(Bucket='mybucket', Key='mykey')
    return response['Body']


def test_objectToDict_succesfull(obj_test):
    data_dict = objectToDict(obj_test) 
    assert data_dict == data_dict_succesfull

def test_objectToDict_succesfull_fail(obj_test_fail):
    data_dict = objectToDict(obj_fail) 
    assert data_dict == data_dict_succesfull
    
def test_validateHash():
    assert validateHash('correct_hash', 'correct_hash') == True
    assert validateHash('correct_hash', 'wrong_hash') == False


def test_getObjectFromBucket(bucket):
    object = getObjectFromBucket(bucket)
    assert is not None

def test_lambdaMain(bucket_name,dynamodb_table):
    test_lambdaMain(bucket_name,dynamodb_table)

@mock_ses
def test_inset_and_delete_doc_succesfull(self):
    #Arrange
    event = json.load(open('resources/event.json'))
    context =  "<bootstrap.LambdaContext object at 0x7fa5db5df9d0>"
    
    s3 = boto3.client("s3")
    #We need to create the bucket since this is all in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket=str(event["Records"][0]['s3']['bucket']['name']))
    s3.put_object(
        Bucket=str(event["Records"][0]['s3']['bucket']['name']), 
        Key= str(event["Records"][0]['s3']['object']['key'])
        )

    client = boto3.client('ses', event["Records"][0]['awsRegion'])

    #Act
    result = lambda_handler(event, context)

    #Assert
    self.assertIn("Objeto eliminado correctamente", result)