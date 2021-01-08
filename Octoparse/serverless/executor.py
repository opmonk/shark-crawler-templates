import json
import postprocessor.parsers.dhgate as dhgate

def main(event, context):

    print("Your numpy array:")
    dhgate.DHGateParser(['-b', '-i', 'input/DHGate-Production-CB-11042020(2).csv', '-o', 'output/', '-p', 'dhgate']).execute()

if __name__ == "__main__":
    main('', '')
