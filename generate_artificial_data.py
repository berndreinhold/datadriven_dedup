#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from numpy import nan

"""
call as: python3 generate_artificial_data.py

Create two tables of artificial data: one with one entry per (project_member_id, date), another with one entry per project_member_id.
There is one class to create one table each.
These tables are saved as csv-files (one per table).
These artificial data can then be used to validate the algorithm of "pairwise processing" and "aggregation of all datasets". The results can visualised as well.

read a config file: 
- provide the number of datasets
- read a matrix relating different datasets to each other for the project_member_id table
- a similar matrix for the number of days 

output:
per_day.csv-files for each dataset containing the correct number of overlapping days and people.


"""




class artificial_datasets():
    def __init__(self, config_filename : str, config_path : str):
        f = open(os.path.join(config_path, config_filename))
        # reading the IO_json config file
        IO_json = json.load(f)
        self.seed = IO_json["seed"]
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.input = IO_json["input"]
        self.matrix_project_member_id = self.input["matrices"]["per_project_member_id"]
        self.matrix_day = self.input["matrices"]["per_day"]
        self.output = IO_json["output"]
        self.count_datasets = IO_json["count_datasets"]
        self.IO_json = IO_json
        self.validate_config_file()

        # output, data to be generated
        self.data = [[]*self.count_datasets]  # this is for every entry (i,j) in the matrix
        self.df_list = []  # this is the list of dataframes, one per dataset
        print(self.data)


    def __del__(self):
        # create output directories and write dataframes to disk
        for df, ds in zip(self.df_list,self.output["datasets"]):
            os.makedirs(os.path.join(self.root_data_dir_name, ds[0]), exist_ok=True)
            df.to_csv(os.path.join(self.root_data_dir_name, ds[0], ds[1]))

    def project_member_ids(self, i : int, j : int):
        pass


    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.IO_json)

    def create_one_dataset(self, i : int, j : int):
        """
        fill self.data[i][j]
        """
        pass


    def loop(self):
        """
        loop through all datasets
        """
        # write csv files
        for i in range(len(self.data)):
            for j in range(len(self.data[i])):
                self.data[i].append(self.create_one_dataset((i,j)))
                # df = pd.DataFrame(data=self.data[i][j])


def main(config_filename : str = "config_artificial_data.json", config_path : str = "."):
    # print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    ad = artificial_datasets(config_filename, config_path)
    ad.loop()

if __name__ == "__main__":
    fire.Fire(main)
