import os
import boto3
import json
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from decimal import Decimal

AWS_OCTOPARSE_DYNAMO_TABLE = os.getenv('AWS_OCTOPARSE_DYNAMO_TABLE')
AWS_REGION = os.getenv('AWS_REGION')

def create_octoparse_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='octoparse-crawls',
        KeySchema=[
            {
                'AttributeName': 'crawl_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'crawl_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def load_crawls(crawls, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')
    for crawl in crawls:
        crawl_id = crawl['crawl_id']
        print("Adding crawl:", crawl_id)
        table.put_item(Item=crawl)

def put_crawl(crawl_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')
    response = table.put_item(
       Item={
            'crawl_id': crawl_id
        }
    )
    return response

def get_crawl(crawl_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')

    try:
        response = table.get_item(Key={'crawl_id': crawl_id})
        #print(response)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

def update_crawl(crawl_id, status, run_token, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')

    response = table.update_item(
        Key={
            'crawl_id': crawl_id
        },
        UpdateExpression="set info.run_token=:r, info.status=:s",
        ExpressionAttributeValues={
            ':s':status,
            ':r': Decimal(run_token)
        },
        ReturnValues="UPDATED_NEW"
    )
    return response

def delete_crawl(crawl_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')

    try:
        response = table.delete_item(
            Key={
                'crawl_id': crawl_id
            }
            #,
            #ConditionExpression="info.status <= :val",
            #ExpressionAttributeValues={
            #    ":val": Decimal(run_token)
            #}
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response

def query_crawls(crawl_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')
    response = table.query(
        KeyConditionExpression=Key('crawl_id').eq(crawl_id)
    )
    print(response)
    return response['Items']

def delete_crawl_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('octoparse-crawls')
    table.delete()

# Test functionality of DynamoDB setup
def setup_crawls_table(dynamodb=None):
    # Delete Table
    delete_crawl_table()
    print("Crawls table deleted.")

    # Create Table
    octoparse_table = create_octoparse_table()
    print("Table status:", octoparse_table.table_status)

    # Load crawls
    with open("crawls.json") as json_file:
        crawl_list = json.load(json_file, parse_float=Decimal)
    load_crawls(crawl_list)

def test_crawls_table(dynamodb=None):
    # Create crawl
    crawl_resp = put_crawl("DHGate-Production-CB-11042020-adidas.csv")
    print("Put crawl succeeded:")
    #pprint(crawl_resp, sort_dicts=False)

    # Read crawl
    crawl = get_crawl("DHGate-Production-CB-11042020(1).csv")
    if crawl:
        print("Get crawl succeeded:")
        print(crawl)

    # Read crawl
    crawl = get_crawl("DHGate-Production-CB-11042020-adidas.csv")
    if crawl:
        print("Get crawl succeeded:")
        print(crawl)

    # Delete crawl
    print("Attempting a conditional delete...")
    delete_response = delete_crawl("DHGate-Production-CB-11042020-adidas.csv")
    if delete_response:
        print("Delete crawl succeeded:")
        print(delete_response)
        #pprint(delete_response, sort_dicts=False)


    # Query crawls
    query_crawl_id = "DHGate-Production-CB-11042020(1).csv"
    print(f"Crawls from {query_crawl_id}")
    crawls = query_crawls(query_crawl_id)
    for crawl in crawls:
        print(crawl['crawl_id'])

if __name__ == '__main__':
    setup_crawls_table()
    test_crawls_table()
