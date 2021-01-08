
class StorageSystem:
    """
    This is an Informal Interface.
    No concrete implementation of methods are done here.
    """
    def get_input_file(self):
        pass

    def set_input_file(self, filename):
        pass

    def get_input_dir(self):
        pass

    def set_input_dir(self, directory):
        pass

    def get_input_file_list(self):
        pass

    def get_output_dir(self):
        pass

    def set_output_dir(self, directory):
        pass

    def get_output_file(self):
        pass

    def set_output_file(self, filename):
        pass

    def set_dataframe(self, filename):
        pass

    def get_dataframe(self):
        pass

    def is_directory(self, directory_name):
        pass

    def generate_file(self, filename):
        pass

    def insert_rows(self, dataframe, filename):
        pass
