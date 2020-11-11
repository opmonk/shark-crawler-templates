#!/usr/bin/python

import subprocess
import time
import os
import sys
import getopt
import csv
import re
import pandas as pd



################
# Reading in command line arguments
################
def main(argv):
   results_file = ''
   p_results_dir = ''
   platform = ''
   try:
      opts, args = getopt.getopt(argv,"hp:i:o:",["platform=","results_file=","p_results_dir="])
   except getopt.GetoptError:
      print('parse_results.py -p <platform> -i <raw inputfile> -o <output directory> -p <platform>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('parse_results.py -i <raw inputfile> -o <output directory> -p <platform>')
         sys.exit()
      elif opt in ("-p", "--platform"):
         platform = arg
      elif opt in ("-i", "--results_file"):
         results_file = arg
      elif opt in ("-o", "--processed_results_dir"):
         p_results_dir = arg
         # Make sure the results directory ends in "/"
         if p_results_dir.rfind("\/") != len(p_results_dir):
             p_results_dir = p_results_dir + "/"
             print(p_results_dir)
   if (results_file == '' or p_results_dir == '' or platform == ''):
      sys.exit(2)
   print('Output directory is "', p_results_dir)
   print('Platform is "', platform)

   if os.path.isdir(results_file):
      print('Raw directory is "', results_file)
      results_dir = results_file
      for results_filename in os.listdir(results_dir):
         if results_dir.rfind("\/") != len(results_dir):
            #results_dir = results_dir + "/"
            print(results_dir)
         results_file = results_dir + results_filename
         print('Raw file is "', results_file)
         parse_results(results_file, p_results_dir, platform)
   else:
      print('Raw file is "', results_file)
      parse_results(results_file, p_results_dir,platform)

def get_run_token(timestamp, keyword, platform ):
   run_token = str(timestamp) + "_" + platform + "_" + keyword
   # print(run_token)
   return run_token

def get_header( results_file ):
   f = open(results_file, 'r')
   header = f.readline()
   f.close()
   return header

def scrub_keyword(keyword):
   # Additional query parameters
   # Aliexpress searchkey ltype
   keyword_trimmed = re.sub(r'&ltype.*','', keyword)
   # DHGate searchkey contains catalog
   keyword_trimmed = re.sub(r'&catalog.*', '', keyword_trimmed)

   # Character encodings:
   # %20 (space) = '+' (needed for filenaming conventions)
   keyword_trimmed = re.sub(r'%20','+', keyword_trimmed)
   # %21 = !
   keyword_trimmed = re.sub(r'%21','!', keyword_trimmed)

   # lower case all the words
   keyword_trimmed = keyword_trimmed.lower()
   return keyword_trimmed

# finds all the searchkey columns in a given raw results file
# e.g., DHGate: searchkey, searchkey2
#       Aliexpress: searchkey
def get_searchkey_columns(results_file):
   df = pd.read_csv(results_file)
   column_names = list(df.columns.values)
   searchkey_list = []
   for column_name in column_names:
       if re.match('searchkey.*', column_name):
           searchkey_list.append(column_name)
   # print (searchkey_list)
   return searchkey_list


# Drop data rows with "" or "Nan" values in the searchkey
def find_unique_searchkey(results_file, searchkey_name):
   nan_value = float("NaN")
   dataframe = pd.read_csv(results_file)
   dataframe.replace("", nan_value, inplace=True)
   dataframe.dropna(subset = [searchkey_name], inplace=True)
   return dataframe[searchkey_name].unique()

# generates map of unique keywords (based on searchkey field) to come up
# with single filename for all variations of searchkey values
def generate_file_keywords_map( results_file ):
   # Some platforms have multiple searchkeys.  Find all searchkeys and
   # then filter to find unique values.
   searchkey_list = get_searchkey_columns(results_file)
   keyword_list = []
   for searchkey in searchkey_list:
      keyword_list.extend(list(find_unique_searchkey(results_file, searchkey)))

   keyword_map = {}
   for keyword in keyword_list:
      keyword_map[scrub_keyword(keyword)] = 1
   # print ("generate_file: ", keyword_map.keys())
   return keyword_map

# generates list of unique searchkey field
def generate_keywords_list( results_file ):
   # Some platforms have multiple searchkeys.  Find all searchkeys and
   # then filter to find unique values.
   searchkey_list = get_searchkey_columns(results_file)
   keyword_list = []
   for searchkey in searchkey_list:
      keyword_list.extend(list(find_unique_searchkey(results_file, searchkey)))

   # keywords are not scrubbed in this case. i.e., we want the keyword list
   # to remain in its raw form for easier filtering later on.
   keyword_map = {}
   for keyword in keyword_list:
      keyword_map[keyword] = 1
   keyword_list = list(keyword_map.keys())
   # print ("generate: ", keyword_list)
   return keyword_list

def parse_results(results_file, p_results_dir,platform):
   # Get new timestamp for each file since a keyword could span
   # across 2 files (i.e., since Octoparse has max limit of 20K rows)
   timestamp = int(str(time.time()).replace('.', ''))

   # Make the results directory
   if not os.path.exists(p_results_dir):
      os.makedirs(p_results_dir)

   headerline = get_header( results_file )

   file_keyword_list = list(generate_file_keywords_map(results_file).keys())
   for filename_keyword in list(file_keyword_list):
       p_results_file = p_results_dir + get_run_token(timestamp, filename_keyword, platform) + ".csv"
       processed_file = open(p_results_file, "w")
       processed_file.write(headerline)
       processed_file.close()

   # Now we need to match keywords found (searchkey) to uniq filenames.
   # searchkey capture is not exact and often relies on proper parsing/scrubbing
   # of the values found in the URLs.
   keyword_list = generate_keywords_list (results_file)

   for keyword in keyword_list:
       filename_keyword = scrub_keyword(keyword)
       p_results_file = p_results_dir + get_run_token(timestamp, filename_keyword, platform) + ".csv"

       # traverse through all the searchkeys to determine which rows to append
       # into split files.
       searchkey_list = get_searchkey_columns(results_file)

       for searchkey_name in searchkey_list:
          df = pd.read_csv(results_file)
          # Apply filter condition based on original searchkey value
          df = df[df[searchkey_name] == keyword]
          df["results_itemnumber"].astype(int)
          # Output of seachkey values must be put into the corresponding filename
          df.to_csv(p_results_file, mode='a', header=False, index=False)

   return

if __name__ == "__main__":
   main(sys.argv[1:])
