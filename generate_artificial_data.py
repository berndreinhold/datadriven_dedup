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
        self.config = json.load(f)
        self.validate_config_file()

    def __del__(self):
        # create directories

        # write csv files
        pass


    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.config)

    def create_one_dataset(self, i, j):
        """

        """
        


    def loop(self):
        """
        loop through all datasets
        """


def main(config_filename : str = "config_artificial_data.json", config_path : str = "."):
    # print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    ad = artificial_datasets(config_filename, config_path)
    ad.create_per()

if __name__ == "__main__":
    fire.Fire(main)
