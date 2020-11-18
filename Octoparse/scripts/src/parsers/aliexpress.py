import sys
from parser import Parser
import re

class AliexpressParser(Parser):
    def __init__(self, argv):
        print(isinstance(self, Parser))
        Parser.__init__(self, argv)

    def scrub_keyword(self, keyword):
        keyword_trimmed = super(AliexpressParser,self).scrub_keyword(keyword)

        # searchkey contains ltype
        keyword_trimmed = re.sub(r'&ltype.*','', keyword_trimmed)
        return keyword_trimmed

    def filter(self, dataframe):
        # Drop rows without an itemnumber (should this be results_url instead?)
        dataframe = dataframe[~dataframe['results_itemnumber'].isnull()]

        # In Aliexpress, for some reason results_itemnumber is being converted to float
        dataframe['results_itemnumber'] = dataframe['results_itemnumber'].astype('int64')
        return dataframe

if __name__=="__main__":
    x = AliexpressParser(sys.argv[1:]).execute()
