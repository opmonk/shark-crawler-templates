import sys
from postprocessor.parsers.parser import Parser
import re

class AmazonParser(Parser):
    def __init__(self, argv):
        print(isinstance(self, Parser))
        Parser.__init__(self, argv)

    def scrub(self, keyword):
        keyword_trimmed = super(AmazonParser,self).scrub(keyword)

        # searchkey contains ltype
        keyword_trimmed = re.sub(r'&ltype.*','', keyword_trimmed)
        return keyword_trimmed

if __name__=="__main__":
    x = AmazonParser(sys.argv[1:]).execute()
