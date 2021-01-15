import json
import sys
import postprocessor.parsers.dhgate as dhgate

def main(payload, context):
    """ Input File can either be .csv or .zip
    Input file should be passed into this function by an event listener.
    .zip => If .zip extension, then need to unzip the file first and place contents in same directory.
            Event listener will then be triggered again since new .csv files appear in this diretory.
    .csv => If .csv extension, then will run through Platform specific parsers.
    """
    print("Input Params:", payload, context)
    #print("Lambda function ARN:", context.invoked_function_arn)
    # Need to parse out input parameters coming from the payload. e.g
    # {'input_file': 'unzip-test/DHGate-Production-CB-11042020%281%29.csv',
    # 'output_dir': 'preprocess/octoparse-dhgate/',
    # 'platform': 'dhgate'}
    # https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
    print(payload['input_file'], payload['output_dir'], payload['platform'])
    platform = payload['platform']
    output_dir = payload['output_dir']
    input_file = payload['input_file']

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
        print("Unrecognized Platform", platform)

if __name__ == "__main__":
    print("Lambda Input Values", sys.argv[1], sys.argv[2])
    main(sys.argv[1], sys.argv[2])
