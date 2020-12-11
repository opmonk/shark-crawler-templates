import json
import src.parsers.dhgate as parse_results


def hello(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    parse_results.main(['-b', '-i', 'input/DHGate-Production-CB-11042020(1).csv', '-o', 'output/', '-p', 'dhgate'])
    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """

def main(event, context):

    print("Your numpy array:")
    parse_results.main(['-b', '-i', 'input/DHGate-Production-CB-11042020(2).csv', '-o', 'output/', '-p', 'dhgate'])


if __name__ == "__main__":
    main('', '')