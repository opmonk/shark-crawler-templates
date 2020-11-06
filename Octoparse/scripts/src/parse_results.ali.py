#!/usr/bin/python

import subprocess
import time
import os
import sys
import getopt
import csv
import re
import pandas as pd

timestamp = int(str(time.time()).replace('.', ''))

################
# Reading in command line arguments
################
def main(argv):
   results_file = ''
   p_results_dir = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["results_file=","p_results_dir="])
   except getopt.GetoptError:
      print('parse_results.py -i <raw inputfile> -o <output directory>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('parse_results.py -i <raw inputfile> -o <output directory>')
         sys.exit()
      elif opt in ("-i", "--results_file"):
         results_file = arg
      elif opt in ("-o", "--processed_results_dir"):
         p_results_dir = arg
         # Make sure the results directory ends in "/"
         if p_results_dir.rfind("\/") != len(p_results_dir):
             p_results_dir = p_results_dir + "/"
             print(p_results_dir)
   if (results_file == '' or p_results_dir == ''):
      sys.exit(2)
   print('Raw file is "', results_file)
   print('Output directory is "', p_results_dir)
   parse_results(results_file, p_results_dir)

def get_run_token( keyword ):
   keyword_trimmed = re.sub(r'&catalog.*', '', keyword)
   run_token = str(timestamp) + "-DHGate-" + keyword_trimmed
   # print(run_token)
   return run_token

def get_header( results_file ):
   f = open(results_file, 'r')
   header = f.readline()
   f.close()
   return header

def scrub_keyword(keyword):
   # Additional query parameters
   keyword_trimmed = re.sub(r'&ltype.*','', keyword)

   # Character encodings:
   # %20 (space) = '+' (needed for filenaming conventions)
   keyword_trimmed = re.sub(r'%20','+', keyword_trimmed)
   # %21 = !
   keyword_trimmed = re.sub(r'%21','!', keyword_trimmed)

   # lower case all the words
   keyword_trimmed = keyword_trimmed.lower()
   return keyword_trimmed

# generates map of unique keywords (based on searchkey field) to come up
# with single filename for all variations of searchkey values
def generate_file_keywords_map( results_file ):
   df = pd.read_csv(results_file)
   keyword_list = list(df.searchkey.unique())
   keyword_map = {}

   for keyword in keyword_list:
      keyword_map[scrub_keyword(keyword)] = 1
   # print ("generate_file: ", keyword_map.keys())
   return keyword_map

# generates list of unique searchkey field
def generate_keywords_list( results_file ):
    df = pd.read_csv(results_file)
    keyword_list = list(df.searchkey.unique())
    # print ("generate: ", keyword_list)
    return keyword_list

def parse_results(results_file, p_results_dir):
   # Make the results directory
   if not os.path.exists(p_results_dir):
      os.makedirs(p_results_dir)

   headerline = get_header( results_file )
   file_keyword_list = list(generate_file_keywords_map(results_file).keys())

   for filename_keyword in list(file_keyword_list):
       p_results_file = p_results_dir + get_run_token(filename_keyword) + ".csv"
       processed_file = open(p_results_file, "w")
       processed_file.write(headerline)
       processed_file.close()

   # Now we need to match keywords found (searchkey) to uniq filenames.
   # searchkey capture is not exact and often relies on proper parsing/scrubbing
   # of the values found in the URLs.
   keyword_list = generate_keywords_list (results_file)
   for keyword in keyword_list:
       filename_keyword = scrub_keyword(keyword)
       p_results_file = p_results_dir + get_run_token(filename_keyword) + ".csv"

       df = pd.read_csv(results_file)
       # Apply filter condition based on original searchkey value
       df = df[df['searchkey'] == keyword]

       # Output of seachkey values must be put into the corresponding filename
       df.to_csv(p_results_file, mode='a', header=False)

   return

if __name__ == "__main__":
   main(sys.argv[1:])
