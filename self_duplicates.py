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
        IO_json = json.load(f)
        self.json_input = IO_json["duplicates_plot"]["input"]
        self.json_output = IO_json["duplicates_plot"]["output"]



        for ds in ["ds1", "ds2"]:
            print(f"{ds}: ", len(self.df[ds]))
            self.df[f"{ds}_groupby"] = self.df[ds].groupby(["date", "user_id", "sgv_mean", "sgv_std", "sgv_min", "sgv_max", "sgv_count"], as_index=False, dropna=False).apply(lambda x: f"{len(x.filename)}, " + ", ".join(x.filename))
            # .agg("count")
            print(f"{ds} after groupby (date, user_id): ", len(self.df[f"{ds}_groupby"]))
            self.df[f"{ds}_groupby"].columns = ["date", "user_id", "sgv_mean","sgv_std", "sgv_min", "sgv_max", "sgv_count", "filenames"]
            self.df[f"{ds}_groupby"].to_csv(f"{ds}_groupby_date_user_id.csv")
            # print(self.df[f"{ds}_groupby"])
            # gui = pdg.show(self.df[f"{ds}_groupby"])

            # test = self.df[f"{ds}_groupby"]
            # print(test["filenames"])
            # test = test.loc[test["filenames"].apply(lambda a: '"' in str(a))]
            # print(test["filenames"].apply(lambda a: '"' in str(a)))
