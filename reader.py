import os
import pandas as pd


class ReadFile:

    dates_list = []

    def __init__(self, corpus_path):
        self.corpus_path = corpus_path
        self.dates_list

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        full_path = os.path.join(self.corpus_path, file_name)
        print(full_path)
        #df = pd.read_parquet(full_path, engine="pyarrow")
        df = pd.read_parquet(self.corpus_path, engine="pyarrow")
        #print(df)
        #self.files_list.append(df.values.tolist())
        return df.values.tolist()

    def create_files_name_list(self):
        self.dates_list = [f.name for f in os.scandir(self.corpus_path) if f.is_dir()]
        return

