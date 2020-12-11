import sys
from src.parsers.parser import Parser
import re


class DHGateParser(Parser):
    def __init__(self, argv):
        print(isinstance(self, Parser))
        Parser.__init__(self, argv)

    def scrub(self, keyword):
        keyword_trimmed = super(DHGateParser,self).scrub(keyword)

        # DHGate searchkey contains catalog
        keyword_trimmed = re.sub(r'&catalog.*', '', keyword_trimmed)
        # %2521 = ! (DHGate - No!no!)
        keyword_trimmed = re.sub(r'%2521','!', keyword_trimmed)

        return keyword_trimmed

def main(args):
    #x = DHGateParser(sys.argv[0:]).execute()
    x = DHGateParser(args[0:]).execute()

if __name__=="__main__":
    main()

