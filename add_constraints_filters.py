#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy
from numpy import nan

"""
call as: python3 add_constraints_filters.py [--config_filename=IO.json] [--config_path="."]

read the data_per_pm_id.csv and modify it with additional constraints and filters.

the concrete constraints are:
- pm_id_1 (OPENonOH Nightscout Uploader) and pm_id_2 (OPENonOH AAPS Uploader) 
share the same project_member_id, however so far only the info from the BG data is exploited.
- same for pm_id_0 (OpenAPS Nightscout Uploader) and pm_id_3 (OpenAPS AAPS Uploader)

recalculate person_id afterwards, belongs_to_datasets, count_belongs_to_datasets.

Perform a outer join of data_per_pm_id.csv with itself based on pm_id_0 == pm_id_3 and pm_id_1 == pm_id_2. 
"""

class add_constraints_filters():
    def __init__(self, config_filename : str, config_path : str):
        f = open(os.path.join(config_path, config_filename))
        # reading the IO_json config file
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.core = IO_json["core"]
        self.steps = IO_json["steps"]
        self.logging = IO_json["logging"]

        # self.output = IO_json["output"]
        self.count_datasets = len(self.core["individual"])
        self.IO_json = IO_json

    def one_merge(self, df1, df2, pm_id_new, pm_id_col1, pm_id_col2):
        df1["pm_id_new_a"] = df1["pm_id_1"]  # now drop pm_id_2
        df2["pm_id_new_a"] = df2["pm_id_2"]  # now drop pm_id_1
        df1.drop(columns=["pm_id_2"], inplace=True)
        df2.drop(columns=["pm_id_1"], inplace=True)
        #df1["pm_id_new_b"] = df1["pm_id_0"]
        #df2["pm_id_new_b"] = df2["pm_id_3"]

        # only relevant are rows where pm_id_new_a or pm_id_new_b are not NaN
        df1 = df1[pd.notnull(df1["pm_id_new_a"])]
        df2 = df2[pd.notnull(df2["pm_id_new_a"])]

        # remove unnecessary columns for merge, keep only the project_member_id variables, 
        # the others are recalculated in the next step 
        df1 = df1[[c for c in df1.columns if "pm_id" in c]]
        df2 = df2[[c for c in df2.columns if "pm_id" in c]]
        
        # apply outer join
        df_res = pd.merge(df1, df2, how="outer", on="pm_id_new_a")

        # combine the other columns (ending on _x and _y)
        # in principle new different pm_id-pairs refering to the same person could arise from the upper constraints

        # drop pm_id_new-column 

        # now df_res also only has the original pm_id_0 to pm_id_3 columns

        return df_res


    def join_them(self):
        """
        self outer join the data_per_pm_id.csv
        """
        # load data_per_pm_id.csv
        df1 = pd.read_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]))
        df2 = pd.read_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]))
        
        # save old data_per_pm_id.csv as data_per_pm_id_before_constraints_filters.csv
        # df1.to_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))

        df_res = self.one_merge(df1, df2, "pm_id_new_a", "pm_id_1", "pm_id_2")
        df_res2 = deepcopy(df_res)
        df_res = self.one_merge(df_res, df_res2, "pm_id_new_b", "pm_id_0", "pm_id_3")

        # calculate and add columns person_id, belongs_to_datasets, count_belongs_to_datasets

        # save data_per_pm_id.csv
        new_outfile_name, ext = os.path.splitext(self.core["output"]["per_pm_id"][1])
        new_outfile_name = new_outfile_name + "_testy" + ext

        df_res.to_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))
        print("saved as: ", os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))


def main(config_filename : str = "config_all.json", config_path : str = ""):
    acf = add_constraints_filters(config_filename, config_path)
    acf.join_them()


if __name__ == "__main__":
    fire.Fire(main)
