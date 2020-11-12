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
    results_file = ''
    p_results_dir = ''
    platform = ''
    timestamp = ''

    def __init__(self, argv):

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
                self.platform = arg
            elif opt in ("-i", "--results_file"):
                self.results_file = arg
            elif opt in ("-o", "--processed_results_dir"):
                self.p_results_dir = arg
                # Make sure the results directory ends in "/"
                if self.p_results_dir.rfind("\/") != len(self.p_results_dir):
                    self.p_results_dir = self.p_results_dir + "/"
                    print(self.p_results_dir)

        if (self.results_file == '' or self.p_results_dir == '' or self.platform == ''):
            print('parse_results.py -i <raw inputfile> -o <output directory> -p <platform>')
            sys.exit(2)

        print('Output directory is: ', self.p_results_dir)
        print('Platform is: ', self.platform)
        print('Input file/directory is: ', self.results_file)

    def execute(self):
        """
        Main Function to execute parse_results.
        """
        print("Main Function: ", self.results_file, self.p_results_dir, self.platform)
        #try:
        if os.path.isdir(self.results_file):
            print('Raw directory is: ', self.results_file)
            results_dir = self.results_file
            for results_filename in os.listdir(results_dir):
                if results_dir.rfind("\/") != len(results_dir):
                    #results_dir = results_dir + "/"
                    print(results_dir)
                    self.results_file = results_dir + results_filename
                    print('Raw file is "', self.results_file)
                    self.parse_results()
        else:
            print('Raw file is "', self.results_file)
            self.parse_results()

    def __get_crawler_id (self):
        """
        This function needs to be replaced with API call to Admin tool
        """
        crawler_id = 0

        if (self.platform.lower() == 'aliexpress'):
            crawler_id = 11
        elif (self.platform.lower() == 'dhgate'):
            crawler_id = 8
        elif (self.platform.lower() == 'bukalapak'):
            crawler_id = 10
        elif (self.platform.lower() == 'lazadacommy'):
            crawler_id = 14
        elif (self.platform.lower() == 'lazadacoid'):
            crawler_id = 15
        return crawler_id

    def __get_run_token(self, filename_keyword, keyword):
        """
        Run token for file naming convention is as follows:
        <timestamp>_<crawlerId>_<assetId>_<keywordId>_<keyword(s)>
        filename_keyword & keyword are needed to generate the right run_token.
        the keyword will need to be matched to the start_url found in the crawler
        table in the Admin Tool
        """

        crawler_id = self.__get_crawler_id()
        # print (str(crawler_id))
        response = requests.get("https://aahhnbypjd.execute-api.us-east-1.amazonaws.com/prod/crawls/metadata?crawler_id=" + str(crawler_id))
        json_crawl_data = json.loads(response.text)

        keyword_id = ""
        asset_id = ""
        #print (response.text)
        for crawl_data in json_crawl_data:
            # !!! Strange behavior.  Throwing TypeError after I run several times.
            # If I comment/uncomment below line, seems to get rid of type error.
            #print (crawl_data["keyword"])

            if (self.encode_keyword(crawl_data["keyword"]).lower() == keyword or
                (self.encode_keyword(crawl_data["start_url"]).lower().find(keyword) != -1 and keyword_id == "")):

                keyword_id = crawl_data["keyword_id"]
                asset_id = crawl_data["asset_id"]
                run_token = str(self.timestamp) + "_" + str(crawler_id) + "_" + str(asset_id) + "_" + str(keyword_id) + "_" + filename_keyword

        if str(keyword_id) == "" or str(asset_id) == "":
            print("ERROR", keyword)
        run_token = str(self.timestamp) + "_" + str(crawler_id) + "_" + str(asset_id) + "_" + str(keyword_id) + "_" + filename_keyword
        return run_token

    def encode_keyword(self, keyword):
        """
        Keywords are stored with spaces.  So need to encode to ensure proper
        comparison with start_urls
        """
        # _ (space) = '+' (needed for keyword comparisons in **json API**)
        keyword_trimmed = re.sub(r' ','+', keyword)
        keyword_trimmed = re.sub(r'!','%21', keyword)
        keyword_trimmed = re.sub(r'\'','%27', keyword)
        return keyword_trimmed

    def __get_header(self):
        f = open(self.results_file, 'r')
        header = f.readline()
        f.close()
        return header

    def scrub_keyword(self, keyword):
        """
        Cleanse/Standardize the keyword for easier comparison to those stored
        in IPShark Admin tool (i.e., start_urls & keyword).

        Args:
        keyword - raw searchkey parameter
        """
        # Character encodings:
        # %20 (space) = '+' (needed for filenaming conventions)
        keyword_trimmed = re.sub(r'%20','+', keyword)


        # %21 = !
        keyword_trimmed = re.sub(r'%21','!', keyword_trimmed)

        # lower case all the words
        keyword_trimmed = keyword_trimmed.lower()
        return keyword_trimmed

    # finds all the searchkey columns in a given raw results file
    # e.g., DHGate: searchkey, searchkey2
    #       Aliexpress: searchkey
    def __get_searchkey_columns(self):
        df = pd.read_csv(self.results_file)
        column_names = list(df.columns.values)
        searchkey_list = []
        for column_name in column_names:
            if re.match('searchkey.*', column_name):
                searchkey_list.append(column_name)
        # print (searchkey_list)
        return searchkey_list

    # Drop data rows with "" or "Nan" values in the searchkey
    def __find_unique_searchkey(self, searchkey_name):
        nan_value = float("NaN")
        dataframe = pd.read_csv(self.results_file)
        dataframe.replace("", nan_value, inplace=True)
        dataframe.dropna(subset = [searchkey_name], inplace=True)
        return dataframe[searchkey_name].unique()

    # generates map of unique keywords (based on searchkey field) to come up
    # with single filename for all variations of searchkey values
    def __generate_file_keywords_map(self):
        # Some platforms have multiple searchkeys.  Find all searchkeys and
        # then filter to find unique values.
        searchkey_list = self.__get_searchkey_columns()
        keyword_list = []
        for searchkey in searchkey_list:
            keyword_list.extend(list(self.__find_unique_searchkey(searchkey)))

            keyword_map = {}
            for keyword in keyword_list:
                keyword_map[self.scrub_keyword(keyword)] = 1
        # print ("generate_file: ", keyword_map.keys())
        return keyword_map

    # generates list of unique searchkey field
    def __generate_keywords_list(self):
        # Some platforms have multiple searchkeys.  Find all searchkeys and
        # then filter to find unique values.
        searchkey_list = self.__get_searchkey_columns()
        keyword_list = []
        for searchkey in searchkey_list:
            keyword_list.extend(list(self.__find_unique_searchkey(searchkey)))

        # keywords are not scrubbed in this case. i.e., we want the keyword list
        # to remain in its raw form for easier filtering later on.
        keyword_map = {}
        for keyword in keyword_list:
            keyword_map[keyword] = 1
        keyword_list = list(keyword_map.keys())
        # print ("generate: ", keyword_list)
        return keyword_list

    def __generate_files(self):
        """
        This function generates the initial filename with the headerline.
        Note: multiple searchkey values can fall into the same filename.
        This depends on how the searchkey values are captured in Octoparse.
        e.g., In DHGate, pagination often strips away some of the page_url
        query parameters, etc.
        """
        headerline = self.__get_header()

        file_keyword_list = list(self.__generate_file_keywords_map().keys())
        for filename_keyword in list(file_keyword_list):
            self.p_results_file = self.p_results_dir + self.__get_run_token(filename_keyword, filename_keyword) + ".csv"
            processed_file = open(self.p_results_file, "w")
            processed_file.write(headerline)
            processed_file.close()

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

        for keyword in keyword_list:
            filename_keyword = self.scrub_keyword(keyword)
            self.p_results_file = self.p_results_dir + self.__get_run_token(filename_keyword, filename_keyword) + ".csv"

            # traverse through all the searchkeys to determine which rows to append
            # into split files.
            searchkey_list = self.__get_searchkey_columns()

            for searchkey_name in searchkey_list:
                df = pd.read_csv(self.results_file)

                df = self.filter(df)

                # Apply filter condition based on original searchkey value
                df = df[df[searchkey_name] == keyword]

                # Output of seachkey values must be put into the corresponding filename
                df.to_csv(self.p_results_file, mode='a', header=False, index=False)

    ################
    # Main function for parsing through raw results and generating individual files.
    ################
    def parse_results(self):
        # Get new timestamp for each file since a keyword could span
        # across 2 files (i.e., since Octoparse has max limit of 20K rows)
        self.timestamp = int(str(time.time()).replace('.', ''))

        # Make the results directory
        if not os.path.exists(self.p_results_dir):
            os.makedirs(self.p_results_dir)

        self.__generate_files()
        self.__insert_rows()

        return


if __name__=="__main__":
    # x = AliexpressParser(sys.argv[1:]).execute()
    Parser(sys.argv[1:]).execute()
