#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy
from numpy import nan, random
import datetime

"""
call as: python3 generate_artificial_data.py

Create two tables of artificial data: one with one entry per (pm_id, date), another with one entry per pm_id.
There is one class to create one table each.
These tables are saved as csv-files (one per table).
These artificial data can then be used to validate the algorithm of "pairwise processing" and "aggregation of all datasets". The results can visualised as well.

read a config file: 
- provide the number of datasets
- read a matrix relating different datasets to each other for the pm_id table
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
        self.date_range = IO_json["date_range"]
        self.matrix_pm_id = IO_json["matrices"]["per_pm_id"]
        self.matrix_day = IO_json["matrices"]["per_day"]
        self.output = IO_json["output"]
        self.count_datasets = IO_json["count_datasets"]
        self.IO_json = IO_json
        self.validate_config_file()

        # output, data to be generated
        self.days = [[]*self.count_datasets]  # this is for every entry (i,j) in the matrix
        self.df_days = []  # this is the list of dataframes, one for every entry (i,j)
        self.df_days_compiled = [] # dataframes, one per dataset (i)


    def __del__(self):
        # create output directories and write dataframes to disk
        for df, ds in zip(self.df_days_compiled,self.output["datasets"]):
            os.makedirs(os.path.join(self.root_data_dir_name, ds[0]), exist_ok=True)
            df.to_csv(os.path.join(self.root_data_dir_name, ds[0], ds[1]))
            print(f"outfiles created: {os.path.join(self.root_data_dir_name, ds[0], ds[1])}")

    def sim_pm_ids(self, i : int, j : int) -> tuple:
        """
        8 digit project member IDs
        """
        count_pm_ids = self.matrix_pm_id[i][j]
        count_days = self.matrix_day[i][j]
        pm_ids1 = self.random.integers(10000000,99999999, count_pm_ids)
        pm_ids2 = self.random.integers(10000000,99999999, count_pm_ids)

        pm_id_indices1 = self.random.integers(0, count_pm_ids, count_days)
        pm_id_indices2 = self.random.integers(0, count_pm_ids, count_days)  # only necessary if not i==j

        pm_id_indices1 = sorted(pm_id_indices1)
        pm_id_indices2 = sorted(pm_id_indices2)  # only necessary if not i==j, but simulated anyway

        project_m_ids1, project_m_ids2 = [],[]
        for k in range(count_days):
            project_m_ids1.append(pm_ids1[pm_id_indices1[k]])
            project_m_ids2.append(pm_ids2[pm_id_indices2[k]])

        return project_m_ids1, project_m_ids2, pm_id_indices1, pm_id_indices2

    def sim_dates(self, i : int, j : int, pm_id_indices1 : list, pm_id_indices2 : list) -> list:

        count_pm_ids = self.matrix_pm_id[i][j]
        count_days = self.matrix_day[i][j]

        # how many days per pm_id:
        dict_count_days1, dict_count_days2 = {}, {}
        for i in range(count_pm_ids):
            dict_count_days1[i] = 0
            dict_count_days2[i] = 0

        for index1, index2 in zip(pm_id_indices1, pm_id_indices2):
            dict_count_days1[index1] += 1
            dict_count_days2[index2] += 1

        # find a start date and a stop date for the number of days:
        min_date = datetime.date.fromisoformat(self.date_range[0])
        max_date = datetime.date.fromisoformat(self.date_range[1])
        max_min_date = max_date - min_date  # assumes that self.date_range[1] > self.date_range[0]

        dict_dates1, dict_dates2 = {}, {}
        for id in dict_count_days1:
            days_offset = int(self.random.integers(0, max_min_date.days-dict_count_days1[id]))
            start_date = min_date + datetime.timedelta(days=days_offset)
            stop_date = start_date + datetime.timedelta(days=dict_count_days1[id])
            dict_dates1[id] = start_date, stop_date, stop_date - start_date

        for id in dict_count_days2:
            days_offset = int(self.random.integers(0, max_min_date.days-dict_count_days2[id]))
            start_date = min_date + datetime.timedelta(days=days_offset)
            stop_date = start_date + datetime.timedelta(days=dict_count_days2[id])
            dict_dates2[id] = start_date, stop_date, stop_date - start_date

        dates = []
        for id in dict_count_days2:
            for a in range(dict_dates2[id][2].days):
                dates.append(dict_dates2[id][0] + datetime.timedelta(a))

        return dates

    def sim_PG_daily_stats(self, count_days) -> tuple:  
        # simulate plasma glucose (PG) statistical variables
        # simulate for all days (count_days), 
        # do not take into account information from actual data, correlations, etc.
        # Possible future improvement:have an individual mean and std per pm_id
        
        PG_mean = self.random.normal(120, 20, count_days)
        PG_std = self.random.normal(20, 15, count_days)
        PG_min, PG_max, PG_count = [], [], []
        min_factor = self.random.normal(3, 1, count_days)
        max_factor = self.random.normal(4, 1, count_days)
        for k in range(count_days):
            PG_min.append(PG_mean[k] - min_factor[k]*PG_std[k])
            PG_max.append(PG_mean[k] + max_factor[k]*PG_std[k])
            PG_count.append(288) # 288: 24h*measurements every 5 min

        return PG_mean, PG_std, PG_min, PG_max, PG_count


    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.IO_json)

    def create_one_dataset(self, i : int, j : int):
        """
        return data per day for one dataset
        PG: plasma glucose
        """
        count_pm_ids = self.matrix_pm_id[i][j]
        count_days = self.matrix_day[i][j]
        if count_days < 1: print(f"Warning: count_days was < 1 ({count_days}) for (i: {i},j: {j})")
        pm_ids1, pm_ids2, pm_id_indices1, pm_id_indices2 = self.sim_pm_ids(i, j)
        dates = self.sim_dates(i, j, pm_id_indices1, pm_id_indices2)

        PG_mean, PG_std, PG_min, PG_max, PG_count = self.sim_PG_daily_stats(count_days)

        # date,sgv_mean,sgv_std,sgv_min,sgv_max,sgv_count,filename,user_id
        data = []
        for k in range(count_days):
            # do a second pm_id column, if not i==j
            if i==j:
                # mean, std, min, max, count, filename, pm_id
                data.append([dates[k], PG_mean[k], PG_std[k], PG_min[k], PG_max[k], PG_count[k], f"test_{i}_{j}.csv", pm_ids1[k]])  
            else:
                data.append([dates[k], PG_mean[k], PG_std[k], PG_min[k], PG_max[k], PG_count[k], f"test_{i}_{j}.csv", pm_ids1[k], pm_ids2[k]])
        return data

    def loop(self):
        """
        loop through all datasets
        simulate all datasets in the matrix (i, j). Only the datasets for i>=j need to be simulated using create_one_dataset(i,j). (2,1) is the same as (1,2), therefore it can be copied.
        """
        # create days for each combination of (i,j) 
        cols = ["date", "sgv_mean", "sgv_std", "sgv_min", "sgv_max", "sgv_count", "filename"]
        for i in range(self.count_datasets):
            self.days.append([])  # data as a list of rows
            self.df_days.append([])  # the corresponding dataframe
            for j in range(self.count_datasets):
                if j >= i: day_data = self.create_one_dataset(i,j)
                else: day_data = self.days[j][i]
                self.days[i].append(day_data)

                cols2 = deepcopy(cols)
                if i==j:
                    cols2.append(f"pm_id_{i}")
                else:
                    cols2.append(f"pm_id_{i}")
                    cols2.append(f"pm_id_{j}")
                df = pd.DataFrame(data=self.days[i][j], columns=cols2)
                cols2 = deepcopy(cols)
                cols2.append(f"pm_id_{i}")
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
                    cols2.append(f"pm_id_{j}")
        """                             


def main(config_filename : str = "config_artificial_data.json", config_path : str = "config"):
    # print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    ad = artificial_datasets(config_filename, config_path)
    ad.loop()

if __name__ == "__main__":
    fire.Fire(main)
