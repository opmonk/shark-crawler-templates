import json
import sys
import postprocessor.parsers.dhgate as dhgate
import postprocessor.parsers.aliexpress as aliexpress
import postprocessor.parsers.bukalapak as bukalapak
from postprocessor.common.octoparse_crawls import OctoparseDynamoDB

def main(event, context):
    """ Input File can either be .csv or .zip
    Input file should be passed into this function by an event listener.
    .zip => If .zip extension, then need to unzip the file first and place contents in same directory.
            Event listener will then be triggered again since new .csv files appear in this diretory.
    .csv => If .csv extension, then will run through Platform specific parsers.
    """
    print("Input Params:", event, context)
    #print("Lambda function ARN:", context.invoked_function_arn)
    # Need to parse out input parameters coming from the event (Payload). e.g
    # {'input_file': 'unzip-test/DHGate-Production-CB-11042020%281%29.csv',
    # 'output_dir': 'preprocess/octoparse-dhgate/',
    # 'platform': 'dhgate'}
    # https://docs.aws.amazon.com/lambda/latest/dg/python-context.html

    # params = json.dumps(event)
    # print("Params: ", params)
    # platform = params['platform']
    # output_dir = params['output_dir']
    # input_file = params['input_file']

    # Check if input is a dictionary already.  Handle incoming requests
    # from handler.js,  __main__ function below, and invoke from command line
    if isinstance(event, dict):
        print("Dictionary:", event['input_file'], event['output_dir'], event['platform'])
        params = event
    else:
        print("JSON: ", event)
        params = json.loads(event)

    platform = str(params['platform'])
    output_dir = str(params['output_dir'])
    input_file = str(params['input_file'])

    # Early exit of function if lambda function has already been called.
    if file_already_processed(input_file):
        print("File is already being processed, Exitting:", input_file)
        return 0

    # Else Continue processing
    if platform == 'dhgate':
        print("DHGate")
        dhgate.DHGateParser(['-b', '-i', input_file, '-o', output_dir, '-p', platform]).execute()
    elif platform == 'aliexpress':
        print("Aliexpress")
        aliexpress.AliexpressParser(['-b', '-i', input_file, '-o', output_dir, '-p', platform]).execute()
    elif platform == 'bukalapak':
        print("Bukalapak")
        bukalapak.BukalapakParser(['-b', '-i', input_file, '-o', output_dir, '-p', platform]).execute()
    else:
        print("ERROR: Unrecognized Platform", platform)
        return 2
    return 0

def file_already_processed(input_file):
    """
    DyanmoDB Table must contain crawlId Item & status attribute
    """
    octoparseDynamoDB = OctoparseDynamoDB()
    crawls = octoparseDynamoDB.query_crawls(input_file)
    if len(crawls) == 0:
        octoparseDynamoDB.put_crawl(input_file)
        octoparseDynamoDB.update_crawl_status(input_file, "scheduled")
        return 0
    else:
        print("Input File is already being processed:", input_file)
        return 1
    # Need to make sure this program is only called once per input file
    # Since this is a lambda function called within a lambda function, need
    # to ensure there is idempotency.  Refer to following document for how
    # to implement with DynamoDB as persistent storage
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html

if __name__ == "__main__":
    #print("Lambda Input Values", sys.argv[1], sys.argv[2])
    event = '{"input_file":"Bukalapak/1142021/Bukalapak-Production-CB-11112020/Bukalapak-Production-CB-11112020.csv","output_dir":"preprocess/octoparse-bukalapak/","platform":"bukalapak"}'
    response = main(event, '')
    print(response)
