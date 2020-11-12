import sys
from parser import Parser
import re

class AliexpressParser(Parser):
    def __init__(self, argv):
        print(isinstance(self, Parser))
        Parser.__init__(self, argv)

    def scrub_keyword(self, keyword):
        keyword_trimmed = super(AliexpressParser,self).scrub_keyword(keyword)

        # Additional query parameters
        # Aliexpress searchkey ltype
        keyword_trimmed = re.sub(r'&ltype.*','', keyword_trimmed)

        # lower case all the words
        keyword_trimmed = keyword_trimmed.lower()
        return keyword_trimmed

if __name__=="__main__":
    x = AliexpressParser(sys.argv[1:]).execute()
