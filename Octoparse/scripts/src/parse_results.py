#!/usr/bin/python

import subprocess
import time
import os 
import sys
import getopt
import csv
import re

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
      print 'parse_results.py -i <raw inputfile> -o <output directory>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'parse_results.py -i <raw inputfile> -o <output directory>'
         sys.exit()
      elif opt in ("-i", "--results_file"):
         results_file = arg
      elif opt in ("-o", "--processed_results_dir"):
         p_results_dir = arg
   if (results_file == '' or p_results_dir == ''):
      sys.exit(2)
   print 'Raw file is "', results_file
   print 'Output directory is "', p_results_dir
   parse_results(results_file, p_results_dir)

def get_run_token( keyword ):
   keyword_trimmed = re.sub(r'&catalog.*', '', keyword)
   run_token = str(timestamp) + "-DHGate-" + keyword_trimmed
   print run_token
   return run_token

def get_header( results_file ):
   f = open(results_file, 'r')
   header = f.readline()
   f.close()
   return header

def generate_keywords_list( results_file ):
   keyword_map = {};
   with open(results_file, 'rt') as inputfile:
      csv_reader = csv.reader(inputfile)

      for line in csv_reader:
         keyword = ''
	 if line[7].strip():
	    keyword = line[7]
         else:
	    keyword = line[8]
         keyword_map[ keyword ] = 1
         if 'searchkey' in keyword_map: del keyword_map['searchkey']
         if 'searchkey2' in keyword_map: del keyword_map['searchkey2']

   return list(keyword_map.keys())

def parse_results(results_file, p_results_dir):
   # Make the results directory
   if not os.path.exists(p_results_dir):
      os.makedirs(p_results_dir)

   headerline = get_header( results_file )
   keyword_list = generate_keywords_list( results_file) 
   
   for keyword in keyword_list:
       p_results_file = p_results_dir + get_run_token( keyword) + ".csv"
       processed_file = open(p_results_file, "w")  
       processed_file.write(headerline) 
       processed_file.close()

   for keyword in keyword_list:
       p_results_file = p_results_dir + get_run_token( keyword) + ".csv"
       processed_file = open(p_results_file, "a")  

       try:
          line = "," + keyword + ",$"
          # subprocess.call(['grep', line , results_file])
          output = subprocess.check_output(['grep', line , results_file])
          if output:
	     processed_file.write(output)
       except subprocess.CalledProcessError:
          print "Not Found"

       try:
          line = "," + keyword + "$"
          # subprocess.call(['grep', line , results_file])
          output = subprocess.check_output(['grep', line , results_file])
          if output:
	     processed_file.write(output)
       except subprocess.CalledProcessError:
          print "Not Found"

       processed_file.close()

       # # look for rows containing the keyword in the searchkey or searchkey2 field
       # grep ",$keyword,$" $results_file >> $p_results_dir/$p_results_file
       # grep ",$keyword$" $results_file >> $p_results_dir/$p_results_file

   return
   
if __name__ == "__main__":
   main(sys.argv[1:])
