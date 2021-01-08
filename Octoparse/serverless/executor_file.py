import json
import postprocessor.parsers.dhgate as dhgate

def main(event, context):

    print("DHGate executing based on File")
    dhgate.DHGateParser(['-i', 'results/raw/DHGate-Production-CB-11042020(2).csv', '-o', 'results/processed/DHGate/file_output', '-p', 'dhgate']).execute()

if __name__ == "__main__":
    main('', '')
