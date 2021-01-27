import json
import sys
import postprocessor.parsers.dhgate as dhgate
import postprocessor.parsers.aliexpress as aliexpress
import postprocessor.parsers.bukalapak as bukalapak
from postprocessor.common.octoparse_crawls import OctoparseDynamoDB

def main(event, context):
    input_file = '../results/raw/zip/DHGate-Production-CB-11042020(2).csv'
    output_dir = '../results/preprocess/file'
    platform = 'dhgate'

    # Early exit of function if lambda function has already been called.
    octoparseDynamoDB = OctoparseDynamoDB()
    if octoparseDynamoDB.put_crawl(input_file) == False:
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


if __name__ == "__main__":
    main('','')
