#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy
from numpy import nan
import logging
from link_all_datasets import entry_datasets_association

"""
call as: python3 add_constraints_filters.py [--config_filename=IO.json] [--config_path="."]

read the data_per_pm_id.csv and modify it with additional constraints and filters.

the concrete constraints are:
- pm_id_1 (OPENonOH Nightscout Uploader) and pm_id_2 (OPENonOH AAPS Uploader) 
share the same project_member_id, however so far only the info from the BG data is exploited.
- same for pm_id_0 (OpenAPS Nightscout Uploader) and pm_id_3 (OpenAPS AAPS Uploader)

recalculate person_id, belongs_to_datasets, count_belongs_to_datasets afterwards.

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


    def one_merge(self, df1 : pd.DataFrame, df2 : pd.DataFrame, pm_id_df1, pm_id_df2, suffix_ : str = ""):
        """
        this requires df1 and df2 to contain NaN for missing values, otherwise the pd.notnull fails.
        However below after the outer join the NaN values are replaced by -1, since NaN == NaN is false, which is undesirable, while -1 == -1 is true, which we want for combining columns.
        
        if suffix_ is not empty, then save dataframe to csv.
        """

        # only relevant are rows where pm_id_col1 or pm_id_col2 are not NaN
        df1 = df1[pd.notnull(df1[pm_id_df1])]
        df2 = df2[pd.notnull(df2[pm_id_df2])]

        # remove unnecessary columns for merge, keep only the project_member_id variables, 
        # the others are recalculated below
        df1 = df1[[c for c in df1.columns if "pm_id" in c]]
        df2 = df2[[c for c in df2.columns if "pm_id" in c]]

        # apply outer join
        df_res = pd.merge(df1, df2, how="outer", left_on=pm_id_df1, right_on=pm_id_df2, suffixes=("_df1", "_df2"))
        
        # replace nan with -1, necessary since NaN == NaN returns False in the selection below
        df_res.fillna(-1, inplace=True)

        # combine the columns that were not joined into one column: pm_id_2_df1 and pm_id_2_df2 into pm_id_2, etc.
        # ending on _df1 and _df2 is introduced by pandas in a merge operation for columns of the same name
        cols_1 = [c for c in df_res.columns if "_df1" in c and not c == pm_id_df1]
        cols_2 = [c for c in df_res.columns if "_df2" in c and not c == pm_id_df2]
        # in principle new different pm_id-pairs refering to the same person could arise from the upper constraints
        # these are caught here via an assert and dealt with, should they arise.
        for c_1, c_2 in zip(cols_1, cols_2):
            # c_1[:-2] == c_2[:-2]  # just the suffixes _1 and _2 are removed
            df_res.loc[(df_res[c_1]==df_res[c_2]),c_1[:-4]] = df_res.loc[(df_res[c_1]==df_res[c_2]),c_1]
            df_res.loc[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] < 0) & (df_res[c_2] > 0),c_1[:-4]] = \
                df_res[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] < 0) & (df_res[c_2] > 0)][c_2]
            df_res.loc[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] > 0) & (df_res[c_2] < 0),c_1[:-4]] = \
                df_res[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] > 0) & (df_res[c_2] < 0)][c_1]
            if ((df_res[c_1]!=df_res[c_2]) & (df_res[c_1] > 0) & (df_res[c_2] > 0)).any():
                df_res.loc[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] > 0) & (df_res[c_2] > 0),[c_1[:-4]]] = \
                    df_res[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] > 0) & (df_res[c_2] > 0)][[c_1, c_2]]
                df_res.loc[(df_res[c_1]!=df_res[c_2]) & (df_res[c_1] > 0) & (df_res[c_2] > 0),["comment"]] = \
                    f"a new pm_id-pair has been found and stored in a list of columns {c_1} and {c_2}."
                logging.warning(f"a new pm_id-pair has been found and stored in a list of columns {c_1} and {c_2}. Please inspect the csv output file.")

        df_res.replace(to_replace=-1, value=nan, inplace=True)

        cols_1.extend(cols_2)
        df_res.drop(columns=cols_1, inplace=True)
        df_res = df_res[sorted(df_res.columns)]

        if len(suffix_) > 0:
            new_outfile_name, ext = os.path.splitext(self.core["output"]["per_pm_id"][1])
            new_outfile_name = new_outfile_name + "_" + suffix_ + ext

            df_res.to_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))
            print("saved as: ", os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))


        # now df_res also only has the original pm_id_0 to pm_id_3 columns
        return df_res


    def join_them(self):
        """
        self outer join the data_per_pm_id.csv
        """
        # load data_per_pm_id.csv
        df1 = pd.read_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]))
        df2 = pd.read_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]))
        
        # save old data_per_pm_id.csv as 
        orig_name, ext = os.path.splitext(self.core["output"]["per_pm_id"][1])
        new_outfile_name = f"{orig_name}_before_constraints_filters{ext}"
        df1.to_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))
        print("saved as: ", os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))

        df_res = self.one_merge(df1, df2, "pm_id_1", "pm_id_2")
        df_res2 = self.one_merge(df1, df2, "pm_id_0", "pm_id_3")
        df_all = pd.merge(df_res, df_res2, how="outer", on=["pm_id_1", "pm_id_2", "pm_id_0", "pm_id_3"], suffixes=("_df1", "_df2"))

        # sort columns
        df_all = df_all[sorted(df_all.columns)]
        assert df_all.drop_duplicates().shape[0] == df_all.shape[0], \
            f"there are duplicate rows in the dataframe: {df_all.drop_duplicates().shape}, {df_all.shape}"

        # calculate and add columns person_id, belongs_to_datasets, count_belongs_to_datasets
        df_all["person_id"] = range(df_all.shape[0])  # here the person_id variable is created. Important!
        entry_datasets_association(df_all)

        new_outfile_name, ext = os.path.splitext(self.core["output"]["per_pm_id"][1])
        new_outfile_name = new_outfile_name + "_total" + ext

        df_all.to_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]))
        print("saved new file as: ", os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]))

        


def main(config_filename : str = "config_master_4ds.json", config_path : str = ""):
    acf = add_constraints_filters(config_filename, config_path)
    acf.join_them()


if __name__ == "__main__":
    fire.Fire(main)
