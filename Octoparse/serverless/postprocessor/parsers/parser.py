import subprocess
import time
import os
import sys
import getopt
import csv
import re
import pandas as pd
import json
import requests
from postprocessor.common.storage import StorageSystem
from postprocessor.common.filesystem import FileSystem
from postprocessor.common.bucket import Bucket

class Parser(object):
    """Parser for Octoparse results.

    Args:
    -h usage
    -i input file/directory
    -o output directory
    -p platform

    Returns:
    Large Raw files will be split into
    their own individual files with the naming convention as follows:
    <timestamp>_<crawlerId>_<assetId>_<keywordId>_<keyword(s)>.csv
    """
    __platform = ''
    __timestamp = ''
    __crawler_id = 0          # Caches crawlerId from API Call
    __crawlers_data = None    # Caches all keywords & start_urls from API Call
    #__is_s3_bucket = False    # Determines if input & output files/dir are s3 buckets

    __storage_system = FileSystem()
    CRAWLERS_API = os.getenv('CRAWLERS_API')
    CRAWLER_ID_API_PREFIX = os.getenv('CRAWLER_ID_API_PREFIX')
    CRAWLER_ID_API_SUFFIX = "/metadata"

    def __init__(self, argv):

        try:
            opts, args = getopt.getopt(argv,"bhp:i:o:",["platform=","results_file=","p_results_dir="])
        except getopt.GetoptError:
            print('parse_results.py [-h] [-b] -i <raw inputfile> -o <output directory> -p <platform>')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('parse_results.py [-h] [-b] -i <raw inputfile> -o <output directory> -p <platform>')
                sys.exit()
            elif opt in ("-p", "--platform"):
                self.__platform = arg
            elif opt in ("-i", "--results_file"):
                self.__storage_system.set_input_file(self.__decode_filename(arg))
            elif opt in ("-o", "--processed_results_dir"):
                self.__storage_system.set_output_dir(arg)
            elif opt in ("-b"):
                #self.__is_s3_bucket = True
                self.__storage_system = Bucket()


        if (self.__storage_system.get_input_file() == '' or
            self.__storage_system.get_output_dir() == '' or
            self.__platform == ''):
            print(self.__storage_system.get_input_file(),self.__storage_system.get_output_dir(),self.__platform)
            print('parse_results.py [-h] [-b] -i <raw inputfile> -o <output directory> -p <platform>')
            sys.exit(2)

        print('Output directory is: ', self.__storage_system.get_output_dir())
        print('Platform is: ', self.__platform)
        print('Input file/directory is: ', self.__storage_system.get_input_file())
        print("Storage System:", type(self.__storage_system))

    def execute(self):
        """
        Main Function to execute parse_results.
        """
        print("Main Function: ", self.__storage_system.get_input_file(), self.__storage_system.get_output_dir(), self.__platform)
        #try:

        if self.__storage_system.is_directory(self.__storage_system.get_input_file()):
            self.__storage_system.set_input_dir(self.__storage_system.get_input_file())
            results_file_list = self.__storage_system.get_input_file_list()
            for results_filename in results_file_list:
                """
                Currently will only be set to directory in filesystem case.
                Need to adjust for S3 Bucket still
                """
                print(results_file_list)
                self.__storage_system.set_input_file(self.__storage_system.get_input_dir() + results_filename)

                # Data Frame is set on a per input file basis.
                self.__storage_system.set_dataframe(self.__storage_system.get_input_dir() + results_filename)         # self.__df required
                print('Raw file is: ', self.__storage_system.get_input_file())
                self.parse_results()

        else:
            print('Raw file is: ', self.__storage_system.get_input_file())
            # Data Frame is set on a per input file basis.
            self.__storage_system.set_dataframe(self.__storage_system.get_input_file())
            self.parse_results()

    def __set_crawler_id (self):
        """
        Call to API should only occur 1 time, check if it exists first.
        """
        crawler_id = 0
        if self.__crawler_id == 0:
            response = requests.get(self.CRAWLERS_API)
            crawlers_data = json.loads(response.text)
            for crawler in crawlers_data:
                if str(crawler["platform"]) == self.__platform:
                    self.__crawler_id = crawler["id"]
                    print ("crawler_id set to: ", self.__crawler_id)

        # Make sure platform is found in API call
        if self.__crawler_id == 0:
            print ("ERROR:", self.__platform, "does not exist in Admin Tool", )
            sys.exit(2)

    def __set_crawlers_data(self):
        if not bool(self.__crawlers_data):
            attempts = 0
            max_attempts = 3
            while attempts < max_attempts:
                response = requests.get(self.CRAWLER_ID_API_PREFIX + str(self.__crawler_id) + self.CRAWLER_ID_API_SUFFIX)
                self.__crawlers_data = json.loads(response.text)
                #print(response.text, response.status_code)
                if response.status_code != 200:
                    print("ERROR:", response.text, attempts)
                    attempts = attempts + 1
                else:
                    attempts = max_attempts

                if response.status_code != 200 and attempts == max_attempts:
                    print("EXITING:", response.text, attempts)
                    sys.exit(2)


    def __get_run_token(self, scrubbed_keyword, keyword):
        """
        Run token for file naming convention is as follows:
        <timestamp>_<crawlerId>_<assetId>_<keywordId>_<keyword(s)>
        scrubbed_keyword & keyword are needed to generate the right run_token.
        the keyword will need to be matched to the start_url found in the crawler
        table in the Admin Tool
        """
        keyword_id = ""
        asset_id = ""
        # priority is assigned for each use case as follows (in order of highest to lowest priority).
        # This is needed to ensure that keywordId & assetId are not overwritten
        # by a lower priority use case.
        #
        # 1) keyword ~= DB_start_url && keyword contains additional query parameters
        # 2) scrubbed_keyword == DB_keyword && keyword ~= DB_start_url
        # 3) scrubbed_keyword == DB_keyword
        # 4) scrubbed_keyword ~= DB_start_url  # Since keyword may not be scrubbed, match to scrubbed keyword
        priority = 100

        for index in range(len(self.__crawlers_data)):
            _db_keyword = self.__encode(self.__crawlers_data[index]["keyword"])
            _db_start_url = self.__encode(self.__crawlers_data[index]["start_url"])

            if (_db_start_url.find(keyword) != -1 and keyword.find('&') != -1):
                priority = 1
                print ("Priority1:", priority, keyword, scrubbed_keyword, _db_start_url, _db_keyword)
                keyword_id = self.__crawlers_data[index]["keyword_id"]
                asset_id = self.__crawlers_data[index]["asset_id"]
            elif (_db_keyword == scrubbed_keyword and _db_start_url.find(keyword) != -1 and priority > 1):
                priority = 2
                print ("Priority2:", priority, keyword, scrubbed_keyword, _db_start_url, _db_keyword)
                keyword_id = self.__crawlers_data[index]["keyword_id"]
                asset_id = self.__crawlers_data[index]["asset_id"]
            elif (_db_keyword == scrubbed_keyword and priority > 2):
                priority = 3
                print ("Priority3:", priority, keyword, scrubbed_keyword, _db_start_url, _db_keyword)
                keyword_id = self.__crawlers_data[index]["keyword_id"]
                asset_id = self.__crawlers_data[index]["asset_id"]
            elif (_db_start_url.find(scrubbed_keyword) != -1 and priority > 3):
                priority = 4
                print ("Priority4:", priority, keyword, scrubbed_keyword, _db_start_url, _db_keyword)
                keyword_id = self.__crawlers_data[index]["keyword_id"]
                asset_id = self.__crawlers_data[index]["asset_id"]
            elif (self.__crawlers_data[-1] == self.__crawlers_data[index] and priority == 100):
                # This is the last element in the json object and no use case was found
                print ("Priority 100:", priority, keyword, scrubbed_keyword, _db_start_url, _db_keyword, _db_start_url.find(keyword))

        run_token = str(self.__timestamp) + "_" + str(self.__crawler_id) + "_" + str(asset_id) + "_" + str(keyword_id) + "_" + scrubbed_keyword
        return run_token

    def __encode(self, keyword):
        """
        API calls to Database contains keywords that are stored with spaces.
        So need to encode keywords to ensure proper comparison with
        Octoparse results.
        """
        # _ (space) = '+' (needed for keyword comparisons in **json API**)
        keyword_trimmed = re.sub(r' ','+', keyword)
        keyword_trimmed = re.sub(r'\'','%27', keyword_trimmed)

        # lower case all the words
        keyword_trimmed = keyword_trimmed.lower()
        return keyword_trimmed

    def __decode_filename(self, keyword):
        """
        Filename needs to be decoded.
        """
        # needed for the input filename. e.g. DHGate-Production-CB-11042020%281%29.csv)
        keyword_trimmed = re.sub(r'%28','(', keyword)
        keyword_trimmed = re.sub(r'%29',')', keyword_trimmed)
        return keyword_trimmed

    def __decode(self, keyword):
        """
        Octoparse results set needs to be decoded.
        """
        # Character encodings:
        # %20 (space) = '+' (needed for filenaming conventions)
        keyword_trimmed = re.sub(r'%20','+', keyword)
        # %21 = !
        keyword_trimmed = re.sub(r'%21','!', keyword_trimmed)

        keyword_trimmed = keyword_trimmed.lower()
        return keyword_trimmed

    def scrub(self, keyword):
        """
        Cleanse/Standardize the keyword for easier comparison to those stored
        in IPShark Admin tool (i.e., start_urls & keyword).  All keywords
        will first be decoded, then it will pass through any platform
        specific scrubbing (e.g., see scrub() method defined in
        dhgate & aliexpress)
        """
        keyword_trimmed = self.__decode(keyword)
        return keyword_trimmed


    def __get_searchkey_columns(self):
        """
        finds all the searchkey columns in a given raw results file
        e.g., DHGate: searchkey, searchkey2
               Aliexpress: searchkey
        """
        df = self.__storage_system.get_dataframe()
        column_names = list(df.columns.values)
        searchkey_list = []
        for column_name in column_names:
            if re.match('searchkey.*', column_name):
                searchkey_list.append(column_name)
        # print (searchkey_list)
        return searchkey_list

    def __find_unique_searchkey(self, searchkey_name):
        """
        Drop data rows with "" or "Nan" values in the searchkey
        """
        nan_value = float("NaN")
        dataframe = self.__storage_system.get_dataframe()
        dataframe.replace("", nan_value, inplace=True)
        dataframe.dropna(subset = [searchkey_name], inplace=True)
        print (searchkey_name)
        return dataframe[searchkey_name].unique()

    def __generate_keywords_list(self):
        """
        generates list of unique searchkey field
        """
        # Some platforms have multiple searchkeys.  Find all searchkeys and
        # then filter to find unique values.
        searchkey_list = self.__get_searchkey_columns()
        keyword_list = []
        for searchkey in searchkey_list:
            keyword_list.extend(list(self.__find_unique_searchkey(searchkey)))
            print(list(self.__find_unique_searchkey(searchkey)))

        print ("generate: ", keyword_list)
        # keywords are not scrubbed in this case. i.e., we want the keyword list
        # to remain in its raw form for easier filtering later on.
        keyword_map = {}
        for keyword in keyword_list:
            keyword_map[keyword] = 1
        keyword_list = list(keyword_map.keys())

        return keyword_list


    def filter(self, dataframe):
        """
        This added filter in place for platforms to do additional filtering on
        the dataframe once the searchkey(s) entries have been found.
        e.g., Aliexpress requires results_itemnumber to be properly formatted
        or values will be converted to float by the pandas library.  Also,
        nan values were found for the results_itemnumber (is this a bug on
        Octoparse task end?)
        """
        return dataframe

    def __insert_rows(self):
        """
        This function does the bulk of the work on inserting the appropriate
        rows based on searchkey field(s) into its corresponding file.
        """
        # Now we need to match keywords found (searchkey) to uniq filenames.
        # searchkey capture is not exact and often relies on proper parsing/scrubbing
        # of the values found in the URLs.
        keyword_list = self.__generate_keywords_list()
        print("Keyword list:", keyword_list)

        for keyword in keyword_list:
            scrubbed_keyword = self.scrub(keyword)

            print("creating files:", scrubbed_keyword, keyword)
            self.__storage_system.set_output_file(self.__get_run_token(scrubbed_keyword, keyword.lower()))
            p_results_file = self.__storage_system.get_output_file()

            # if self.__is_s3_bucket:
            #     p_results_file = self.__get_run_token(scrubbed_keyword, keyword.lower()) + ".csv"
            # else:
            #     p_results_file = self.__storage_system.get_output_dir() + self.__get_run_token(scrubbed_keyword, keyword.lower()) + ".csv"
            print("Get Run Token:", p_results_file)

            # If file does not exist, then need to create a new file with header
            # row.
            self.__storage_system.generate_file(p_results_file)

            # traverse through all the searchkeys to determine which rows to append
            # into split files.
            searchkey_list = self.__get_searchkey_columns()

            for searchkey_name in searchkey_list:
                df = self.__storage_system.get_dataframe()
                df = self.filter(df)

                # Apply filter condition based on original searchkey value
                df = df[df[searchkey_name] == keyword]
                # Output of seachkey values must be put into the corresponding filename
                self.__storage_system.insert_rows(df,p_results_file)

                #df.to_csv(p_results_file, mode='a', header=False, index=False)

    def parse_results(self):
        """
        Main function for parsing through raw results and generating individual files.
        """
        # Get new timestamp for each file since a keyword could span
        # across 2 files (i.e., since Octoparse has max limit of 20K rows)
        self.__timestamp = int(str(time.time()).replace('.', ''))

        # API Calls: Crawler Id needs to be set prior to setting
        # crawlers_data (i.e., crawler_id is needed to obtain crawlers_data)
        self.__set_crawler_id()         # self.__platform required
        self.__set_crawlers_data()      # self.__crawler_id required

        self.__insert_rows()

        return


if __name__=="__main__":
    Parser(sys.argv[1:]).execute()
