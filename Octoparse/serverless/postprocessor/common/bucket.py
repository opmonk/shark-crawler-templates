import boto3
import s3fs
import os
from io import StringIO
from postprocessor.common.storage import StorageSystem
import pandas as pd

class Bucket(StorageSystem):
    __input_file = ""           # e.g "input/DHGate.csv"
    __input_dir = ""
    __input_file_list = []
    __output_directory = ""     # e.g "output/"
    __output_file = ""
    __df = None                 # dataframe

    # Used with input_file
    # https://www.goingserverless.com/blog/using-environment-variables-with-the-serverless-framework
    AWS_OCTOPARSE_RAW_BUCKET_NAME = os.getenv('AWS_OCTOPARSE_RAW_BUCKET_NAME')

    # Used with output_file
    AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME = os.getenv('AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME')


    def get_input_file(self):
        return self.__input_file

    def set_input_file(self, filename):
        self.__input_file = filename
        #self.__input_file = filename

    def get_input_dir(self):
        return self.__input_dir

    def set_input_dir(self, directory):
        pass

    def get_input_file_list(self):
        pass

    def get_output_dir(self):
        return self.__output_directory

    def set_output_dir(self, directory):
        self.__output_directory = directory

    def get_output_file(self):
        return self.__output_file

    def set_output_file(self, run_token):
        self.__output_file = run_token + ".csv"


    def set_dataframe(self, filename):
        """
        Filename is passed in as parameter.  This is needed in case the input_file
        was specified as a directory.
        """
        print ("S3 Storage:", "s3://" + self.AWS_OCTOPARSE_RAW_BUCKET_NAME + "/" + filename)
        self.__df= pd.read_csv("s3://" + self.AWS_OCTOPARSE_RAW_BUCKET_NAME + "/" + filename)

    def get_dataframe(self):
        print ("GETTING DataFrame", self.__input_file)
        self.__df= pd.read_csv("s3://" + self.AWS_OCTOPARSE_RAW_BUCKET_NAME + "/" + self.__input_file)
        return self.__df

    def is_directory(self, directory_name):
        """
        TBD
        """
        return False

    def __read_header(self):
        """
        Reading input_file headerline
        """
        s3_resource = boto3.resource('s3')
        obj = s3_resource.Object(self.AWS_OCTOPARSE_RAW_BUCKET_NAME, self.__input_file)
        header = obj.get()['Body']._raw_stream.readline()
        return header

    def generate_file(self, filename):
        """
        First check if this is a new file to be generated:
        """
        s3_resource = boto3.resource('s3')
        try:
            csv_prev_content = str(s3_resource.Object(self.AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME, self.__output_directory + filename).get()['Body'].read(), 'utf8')
        except:
            csv_prev_content = ''

        # If prev content is empty, this is a new file.
        if csv_prev_content == '':
            headerline = self.__read_header()
            s3_resource = boto3.resource('s3')
            s3_resource.Object(self.AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME, self.__output_directory + filename).put(Body=headerline)


    def insert_rows(self, dataframe, filename):
        csv_buffer = StringIO()
        s3_resource = boto3.resource('s3')
        try:
            csv_prev_content = str(s3_resource.Object(self.AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME, self.__output_directory + filename).get()['Body'].read(), 'utf8')
        except:
            csv_prev_content = ''
        dataframe.to_csv(csv_buffer, header=False, index=False)
        csv_output = csv_prev_content + csv_buffer.getvalue()

        # how do i know if header info got in there.
        s3_resource.Object(self.AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME, self.__output_directory + filename).put(Body=csv_output)
