from src.common.storage import StorageSystem
import os
import pandas as pd

class FileSystem(StorageSystem):
    __input_file = ""
    __input_dir = ""
    __input_file_list = []
    __output_directory = ""
    __output_file = ""
    __df = None

    def get_input_file(self):
        return self.__input_file

    def set_input_file(self, filename):
        self.__input_file = filename

    def get_input_dir(self):
        return self.__input_dir

    def set_input_dir(self, directory):
        self.__input_dir = directory

    def get_input_file_list(self):
        return os.listdir(self.__input_file)

    def get_output_dir(self):
        return self.__output_directory

    def set_output_dir(self, directory):
        # Make sure the results directory ends in "/"
        if directory.rfind("\/") != len(directory):
            self.__output_directory = directory + "/"
        else:
            self.__output_directory = directory
        # Make the results directory
        if not os.path.exists(self.__output_directory):
            os.makedirs(self.__output_directory)
        print(self.__output_directory)

    def get_output_file(self):
        return self.__output_file

    def set_output_file(self, filename):
        self.__output_file = filename

    def set_dataframe(self, filename):
        """
        Filename is passed in as parameter.  This is needed in case the input_file
        was specified as a directory.
        """
        self.__df = pd.read_csv(filename)

    def get_dataframe(self):
        #print ("GETTING DataFrame", self.__df)
        self.__df = pd.read_csv(self.__input_file)
        return self.__df

    def is_directory(self, directory_name):
        return os.path.isdir(directory_name)

    def __read_header(self):
        """
        Reading input_file headerline
        """
        f = open(self.__input_file, 'r')
        header = f.readline()
        f.close()
        return header

    def generate_file(self, filename):
        if not os.path.isfile(filename):
            print("Adding To file: ", filename)
            headerline = self.__read_header()
            processed_file = open(filename, "w")
            processed_file.write(headerline)
            processed_file.close()

    def insert_rows(self, dataframe, filename):
        print("Adding To file")
        dataframe.to_csv(filename, mode='a', header=False, index=False)
