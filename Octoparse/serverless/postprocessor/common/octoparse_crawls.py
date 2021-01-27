import os
import boto3
import json
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from decimal import Decimal

AWS_OCTOPARSE_DYNAMO_TABLE = os.getenv('AWS_OCTOPARSE_DYNAMO_TABLE')
AWS_REGION = os.getenv('AWS_REGION')
#dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(AWS_OCTOPARSE_DYNAMO_TABLE)

class OctoparseDynamoDB():
    def create_octoparse_table(self):
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

    def load_crawls(self, crawls):
        for crawl in crawls:
            crawl_id = crawl['crawl_id']
            print("Adding crawl:", crawl_id)
            table.put_item(Item=crawl)

    def put_crawl(self, crawl_id):
        response = table.put_item(
           Item={
                'crawl_id': crawl_id
            }
        )
        return response

    def get_crawl(self, crawl_id):
        try:
            response = table.get_item(Key={'crawl_id': crawl_id})
            print("GET CRAWL:", response['Item'])
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response['Item']

    def update_crawl_status(self, crawl_id, statuses):
        try:
            response = table.update_item(
                Key={
                    'crawl_id': crawl_id
                },
                UpdateExpression="set info.statuses=:s",
                ExpressionAttributeValues={
                    ':s': statuses
                },
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ValidationException':
              # Creating new top level attribute `info` (with nested props)
              # if the previous query failed
              response = table.update_item(
                  Key={
                      "crawl_id": crawl_id
                  },
                  UpdateExpression="set #attrName = :attrValue",
                  ExpressionAttributeNames = {
                      "#attrName" : "info"
                  },
                  ExpressionAttributeValues={
                      ':attrValue': {
                          'statuses': statuses
                      }
                  },
                  ReturnValues="UPDATED_NEW"
              )
            else:
              raise
        print("UP STATUS:",response)
        return response

    def update_crawl_run_token(self, crawl_id, run_token):
        response = table.update_item(
            Key={
                'crawl_id': crawl_id
            },
            UpdateExpression="set info.run_token=:r",
            ExpressionAttributeValues={
                ':r': run_token
            },
            ReturnValues="UPDATED_NEW"
        )

        print("UP RUN TOKEN:",response)
        return response

    def delete_crawl(self, crawl_id):
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
            print(response)
            return response


    def query_crawls(self, crawl_id):
        response = table.query(
            KeyConditionExpression=Key('crawl_id').eq(crawl_id)
        )
        print(response)
        return response['Items']

    def delete_crawl_table(self):
        table.delete()

    # Test functionality of DynamoDB setup
    def setup_crawls_table(self):
        # Delete Table
        self.delete_crawl_table()
        print("Crawls table deleted.")

        # Create Table
        octoparse_table = self.create_octoparse_table()
        print("Table status:", octoparse_table.table_status)

        # Load crawls
        with open("octoparse_crawls.json") as json_file:
            crawl_list = json.load(json_file, parse_float=Decimal)
        self.load_crawls(crawl_list)

    def test_crawls_table(self):
        # Create crawl
        crawl_resp = self.put_crawl("DHGate-Production-CB-11042020-adidas.csv")

        # Read crawl
        crawl = self.get_crawl("DHGate-Production-CB-11042020-adidas.csv")
        crawl_resp = self.update_crawl_status("DHGate-Production-CB-11042020-adidas.csv", "scheduled")
        crawl_resp = self.update_crawl_run_token("DHGate-Production-CB-11042020-adidas.csv", 1234)
        crawl_resp = self.update_crawl_status("DHGate-Production-CB-11042020-adidas.csv", "complete")
        crawl = self.get_crawl("DHGate-Production-CB-11042020-adidas.csv")

        # Delete crawl
        #print("Attempting a conditional delete...")
        #delete_response = self.delete_crawl("DHGate-Production-CB-11042020-adidas.csv")

        # Query crawls
        query_crawl_id = "DHGate-Production-CB-11042020(1).csv"
        print(f"Crawls from {query_crawl_id}")
        crawls = self.query_crawls(query_crawl_id)
        print(crawls)

if __name__ == '__main__':
    octoparse_db = OctoparseDynamoDB()
    octoparse_db.test_crawls_table()
    #setup_crawls_table()
    #octoparse_table = create_octoparse_table()
    #print("Table status:", octoparse_table.table_status)
    #test_crawls_table()
