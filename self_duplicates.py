#/usr/bin/env python3

import pandas as pd
import os
import json
import fire


"""
    identify and remove self-duplicates. Self-duplicates are duplicate (days, user_id)-pairs within one dataset.
    We found for example in the OPENonOH dataset:
    number of occurrences of duplicates, frequency of (date, user_id) in the dataset
        2781, 2
        49, 3
        77, 4
        24, 5
        27, 6
        165, 7
        173, 8
    e.g. 2781 (day,user_id)-pairs occur 2 times in the OPENonOH dataset. 173 (day, user_id)-pairs occur even 8 times in the dataset.

    These (day,user_id)-duplicates are reduced to appear only once in the dataset.
"""


class self_duplicates(object):

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [user_id, date]
            - OPENonOH with key [user_id, date]
            - duplicates file containing the duplicates between the two data files with key [user_id_ds1, user_id_ds2, date]
        """
        f = open(os.path.join(config_path, config_filename))
        self.IO_json = json.load(f)["self_duplicates"]
        self.datasets = self.IO_json.keys()
        self.sgv_columns = ["sgv_mean", "sgv_std", "sgv_min", "sgv_max", "sgv_count"]  # sgv: single (?) glucose value (see json files)

        self.df = {}
        for ds in self.datasets:
            self.df[ds] = pd.read_csv(os.path.join(self.IO_json[ds]["dir_name"], self.IO_json[ds]["in_file_name"]), header=0, parse_dates=[1], index_col=0)
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            # fix the data types that were loaded as the unspecific "object"
            for col in self.df[ds].columns:
                if "filename" in col:
                    self.df[ds][col] = self.df[ds][col].astype('string')


    def list_self_duplicates(self, dataset : str) -> pd.DataFrame:        
        print(f"{dataset}: {len(self.df[dataset])} days")
        # self.df[f"{dataset}_groupby"] = self.df[dataset][["date", "user_id", "filename"]].groupby(["date", "user_id"], as_index=False, dropna=False).agg("count")
        df_gb = self.df[dataset][["date", "user_id", "filename"]].groupby(["date", "user_id"], as_index=False, dropna=False).agg("count")
        
        # the following needs to be done in two steps: first: ", ".join(x), then: x.split(",")[0]
        df_gb = self.df[dataset][["date", "user_id", "filename"]].groupby(["date", "user_id"], as_index=False, dropna=False).agg(count_fn= ('filename', 'count'), groupby_filename=('filename', lambda x: ", ".join(x)))
        df_gb = df_gb.apply({'date' : lambda x: x, 'user_id' : lambda x: x, 'count_fn' : lambda x: x, 'groupby_filename': lambda x: x.split(",")[0]})
        df_gb["one_filename"] = df_gb["groupby_filename"]
        df_gb = df_gb[["date", "user_id", "count_fn", "one_filename"]]
        
        print(f"{dataset} after groupby (date, user_id): {len(df_gb)} days")
        df_gb.sort_values(by=['user_id', 'date'], inplace=True)
        return df_gb

    def loop(self):
        for dataset in self.datasets:
            df_groupby = self.list_self_duplicates(dataset)
            self.clean_self_duplicates(df_groupby, dataset)

    def clean_self_duplicates(self, df_groupby : pd.DataFrame, dataset : str):
        df_merged = pd.merge(self.df[dataset], df_groupby, how='inner', left_on=["date", "user_id", "filename"], right_on=["date", "user_id", "one_filename"])
        df_merged = df_merged[self.df[dataset].columns]
        df_merged.to_csv(os.path.join(self.IO_json[dataset]["dir_name"], self.IO_json[dataset]["out_file_name"]))


def main(config_filename : str = "IO.json", config_path : str = "."):

    sd = self_duplicates(config_filename, config_path)
    sd.loop()
    

if __name__ == "__main__":
    main()