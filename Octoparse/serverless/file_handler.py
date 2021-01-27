import json
import sys
import postprocessor.parsers.dhgate as dhgate
import postprocessor.parsers.aliexpress as aliexpress
import postprocessor.parsers.bukalapak as bukalapak
from postprocessor.common.octoparse_crawls import OctoparseDynamoDB

def main(event, context):
    input_file = '../results/raw/1142021/Bukalapak-Production-CB-11112020.csv'
    output_dir = '../results/preprocess/'
    platform = 'bukalapak'

    # Early exit of function if lambda function has already been called.
    if file_already_processed(input_file):
        print("File is already being processed, Exitting:", input_file)
        return 0

    if platform == 'dhgate':
        print("DHGate")
        dhgate.DHGateParser(['-i', input_file, '-o', output_dir, '-p', platform]).execute()
    elif platform == 'aliexpress':
        print("Aliexpress")
        aliexpress.AliexpressParser(['-i', input_file, '-o', output_dir, '-p', platform]).execute()
    elif platform == 'bukalapak':
        print("Bukalapak")
        bukalapak.BukalapakParser(['-i', input_file, '-o', output_dir, '-p', platform]).execute()
    else:
        print("Unrecognized Platform", platform)

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

if __name__ == "__main__":
    main('','')
