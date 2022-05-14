#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy
from numpy import nan, random

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
        seed = IO_json["seed"]
        if len(seed) == 0 or seed == "None":
            seed = None
        else:
            seed = int(seed)
        self.random = random.default_rng(seed)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.matrix_project_member_id = IO_json["matrices"]["per_project_member_id"]
        self.matrix_day = IO_json["matrices"]["per_day"]
        self.output = IO_json["output"]
        self.count_datasets = IO_json["count_datasets"]
        self.IO_json = IO_json
        self.validate_config_file()

        # output, data to be generated
        self.days = [[]*self.count_datasets]  # this is for every entry (i,j) in the matrix
        self.df_days = []  # this is the list of dataframes, one per dataset
        self.df_days_compiled = []


    def __del__(self):
        # create output directories and write dataframes to disk
        for df, ds in zip(self.df_days_compiled,self.output["datasets"]):
            os.makedirs(os.path.join(self.root_data_dir_name, ds[0]), exist_ok=True)
            df.to_csv(os.path.join(self.root_data_dir_name, ds[0], ds[1]))
            print(f"outfiles created: {os.path.join(self.root_data_dir_name, ds[0], ds[1])}")

    def project_member_ids(self, i : int, j : int):
        count_project_member_ids = int(self.matrix_project_member_id[i][j])
        pm_ids1 = self.random.integers(10000000,99999999, count_project_member_ids)
        pm_ids2 = self.random.integers(10000000,99999999, count_project_member_ids)
        return pm_ids1, pm_ids2  # random 8 bit number

    def sim_PG_one_day() -> tuple:  # PG: plasma glucose
        pass


    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.IO_json)

    def create_one_dataset(self, i : int, j : int):
        """
        return data per day for one dataset
        PG: plasma glucose
        """
        count_project_member_ids = self.matrix_project_member_id[i][j]
        count_days = self.matrix_day[i][j]
        pm_ids1, pm_ids2 = self.project_member_ids(i,j)  # len(pm_ids1) == count_project_member_ids
        pm_id_indices1 = self.random.integers(0, count_project_member_ids, count_days)
        pm_id_indices2 = self.random.integers(0, count_project_member_ids, count_days)  # only necessary if not i==j

        pm_id_indices1 = sorted(pm_id_indices1)
        pm_id_indices2 = sorted(pm_id_indices2)  # only necessary if not i==j, but simulated anyway

        PG_mean = self.random.normal(120, 20, count_days)
        PG_std = self.random.normal(20, 15, count_days)
        PG_min, PG_max = [],[]
        min_factor = self.random.normal(3, 1, count_days)
        max_factor = self.random.normal(4, 1, count_days)
        for k in range(count_days):
            PG_min.append(PG_mean[k] - min_factor[k]*PG_std[k])
            PG_max.append(PG_mean[k] + max_factor[k]*PG_std[k])

        # date,sgv_mean,sgv_std,sgv_min,sgv_max,sgv_count,filename,user_id
        data = []
        for k in range(count_days):
            # do a second project_member_id column, if not i==j
            if i==j:
                project_member_id = pm_ids1[pm_id_indices1[k]]
                # mean, std, min, max, count, filename, project_member_id
                data.append(["2021-05-35", PG_mean[k], PG_std[k], PG_min[k], PG_max[k], 288, f"test_{i}_{j}.csv", project_member_id])  # 288: 24h*measurements every 5 min
            else:
                data.append(["2021-05-35", PG_mean[k], PG_std[k], PG_min[k], PG_max[k], 288, f"test_{i}_{j}.csv", pm_ids1[pm_id_indices1[k]], pm_ids2[pm_id_indices2[k]]])
        return data

    def loop(self):
        """
        loop through all datasets
        """
        # create days for each combination of (i,j) 
        cols = ["date", "sgv_mean", "sgv_std", "sgv_min", "sgv_max", "sgv_count", "filename"]
        for i in range(self.count_datasets):
            self.days.append([])
            self.df_days.append([])
            for j in range(self.count_datasets):
                self.days[i].append(self.create_one_dataset(i,j))
                if i==j:
                    cols2 = deepcopy(cols)
                    cols2.append(f"project_member_id_{i}")
                else:
                    cols2 = deepcopy(cols)
                    cols2.append(f"project_member_id_{i}")
                    cols2.append(f"project_member_id_{j}")
                df = pd.DataFrame(data=self.days[i][j], columns=cols2)
                cols2 = deepcopy(cols)
                cols2.append(f"project_member_id_{i}")
                self.df_days[i].append(df[cols2])

        # mix them:
        self.df_days_compiled = []*self.count_datasets  # compiled across the different tuples self.df_days[i][j] all become aggregated into self.df_days[i]
        for i in range(self.count_datasets):
            self.df_days_compiled.append(pd.concat(self.df_days[i][:], axis=0))

        """
            self.df_days_compiled.append([])
            for j in range(self.count_datasets):
                if j == i:
                    self.df_days_compiled[i] = self.df_days[i][i]
                elif j > i:
                    cols2 = cols
                    cols2.append(f"project_member_id_{j}")
        """                             


def main(config_filename : str = "config_artificial_data.json", config_path : str = "."):
    # print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    ad = artificial_datasets(config_filename, config_path)
    ad.loop()

if __name__ == "__main__":
    fire.Fire(main)
