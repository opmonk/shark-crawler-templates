import sys
from parser import Parser
import re

class BukalapakParser(Parser):
    def __init__(self, argv):
        print(isinstance(self, Parser))
        Parser.__init__(self, argv)

if __name__=="__main__":
    x = BukalapakParser(sys.argv[1:]).execute()
