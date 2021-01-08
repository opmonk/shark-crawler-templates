import json
import postprocessor.parsers.dhgate as dhgate

def main(self, argv):
    """ Input File can either be .csv or .zip
    Input file should be passed into this function by an event listener.
    .zip => If .zip extension, then need to unzip the file first and place contents in same directory.
            Event listener will then be triggered again since new .csv files appear in this diretory.
    .csv => If .csv extension, then will run through Platform specific parsers.
    """
    print("Your Input File:")
    dhgate.DHGateParser(['-b', '-i', 'input/moon_child_glow.csv', '-o', 'output_moon/', '-p', 'dhgate']).execute()

if __name__ == "__main__":
    main(sys.argv[1:])
