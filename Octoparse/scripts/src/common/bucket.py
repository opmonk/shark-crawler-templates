import boto3
import s3fs
from io import StringIO
from common.storage import StorageSystem
import pandas as pd

class Bucket(StorageSystem):
    __input_file = ""           # e.g "input/DHGate.csv"
    __input_dir = ""
    __input_file_list = []
    __output_directory = ""     # e.g "output/"
    __output_file = ""
    __df = None                 # dataframe
    AWS_S3_BUCKET = "ipshark-test-temp"

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

    def set_output_file(self, filename):
        self.__output_file = filename

    def set_dataframe(self, filename):
        """
        Filename is passed in as parameter.  This is needed in case the input_file
        was specified as a directory.
        """
        print ("S3 Storage:", "s3://" + self.AWS_S3_BUCKET + "/" + filename)
        self.__df= pd.read_csv("s3://" + self.AWS_S3_BUCKET + "/" + filename)

    def get_dataframe(self):
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
        obj = s3_resource.Object(self.AWS_S3_BUCKET, self.__input_file)
        header = obj.get()['Body']._raw_stream.readline()
        return header

    def generate_file(self, filename):
        headerline = self.__read_header()
        s3_resource = boto3.resource('s3')
        s3_resource.Object(self.AWS_S3_BUCKET, self.__output_directory + filename).put(Body=headerline)

    def insert_rows(self, dataframe, filename):
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')

        # how do i know if header info got in there.
        s3_resource.Object(self.AWS_S3_BUCKET, self.__output_directory + filename).put(Body=csv_buffer.getvalue())
