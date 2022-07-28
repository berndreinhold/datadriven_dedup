#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy
from numpy import nan
import sqlite3

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

        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row  # getting column values by name: https://docs.python.org/3/library/sqlite3.html#sqlite3.Row
        self.cur = self.con.cursor()
        self.cur.execute("CREATE TABLE data_per_pm_id (pm_id_0 INTEGER, pm_id_1 INTEGER, pm_id_2 INTEGER, pm_id_3 INTEGER, person_id INTEGER, belongs_to_datasets INTEGER, count_belongs_to_datasets INTEGER)")

    def one_merge(self, df1, df2, pm_id_new, pm_id_col1, pm_id_col2):
        df1[pm_id_new] = df1[pm_id_col1]  # now drop pm_id_2
        df2[pm_id_new] = df2[pm_id_col2]  # now drop pm_id_1
        df1.drop(columns=[pm_id_col2], inplace=True)
        df2.drop(columns=[pm_id_col1], inplace=True)
        #df1["pm_id_new_b"] = df1["pm_id_0"]
        #df2["pm_id_new_b"] = df2["pm_id_3"]

        # only relevant are rows where pm_id_new_a or pm_id_new_b are not NaN
        df1 = df1[pd.notnull(df1[pm_id_new])]
        df2 = df2[pd.notnull(df2[pm_id_new])]

        # remove unnecessary columns for merge, keep only the project_member_id variables, 
        # the others are recalculated in the next step 
        df1 = df1[[c for c in df1.columns if "pm_id" in c]]
        df2 = df2[[c for c in df2.columns if "pm_id" in c]]

        # replace nan with -1, necessary since NaN == NaN returns False in the selection below
        df1 = df1.apply(lambda x: x.fillna(-1), axis=0)
        df2 = df2.apply(lambda x: x.fillna(-1), axis=0)

        # apply outer join
        df_res = pd.merge(df1, df2, how="outer", on=pm_id_new)
        df_res.fillna(-1, inplace=True)

        # combine the other columns (ending on _x and _y) into one column: pm_id_2_x and pm_id_2_y into pm_id_2, etc.
        cols_x = [c for c in df_res.columns if "_x" in c]
        cols_y = [c for c in df_res.columns if "_y" in c]
        # in principle new different pm_id-pairs refering to the same person could arise from the upper constraints
        # these are caught here via an assert and dealt with, should they arise.
        for c_x, c_y in zip(cols_x, cols_y):
            df_res.loc[(df_res[c_x]==df_res[c_y]),c_x[:-2]] = df_res.loc[(df_res[c_x]==df_res[c_y]),c_x]
            df_res.loc[(df_res[c_x]!=df_res[c_y]) & (df_res[c_x] < 0) & (df_res[c_y] > 0),c_x[:-2]] = \
                df_res[(df_res[c_x]!=df_res[c_y]) & (df_res[c_x] < 0) & (df_res[c_y] > 0)][c_y]
            df_res.loc[(df_res[c_x]!=df_res[c_y]) & (df_res[c_x] > 0) & (df_res[c_y] < 0),c_x[:-2]] = \
                df_res[(df_res[c_x]!=df_res[c_y]) & (df_res[c_x] > 0) & (df_res[c_y] < 0)][c_x]
            assert len(df_res[(df_res[c_x]!=df_res[c_y]) & (df_res[c_x] > 0) & (df_res[c_y] > 0)]) == 0

        # drop pm_id_new-column 
        cols_x.extend(cols_y)
        cols_x.extend([pm_id_new])
        df_res.drop(columns=cols_x, inplace=True)
        
        # now df_res also only has the original pm_id_0 to pm_id_3 columns
        return df_res

    def fill_sql_table(self):
        df1 = pd.read_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], self.core["output"]["per_pm_id"][1]), header=0)
        data_ = []
        for row in df1.itertuples():
            data_.append([row.pm_id_0, row.pm_id_1, row.pm_id_2, row.pm_id_3, row.person_id, row.belongs_to_datasets, row.count_belongs_to_datasets])
        
        self.cur.executemany("Insert into data_per_pm_id values (?,?,?,?,?,?,?)", data_)
        print(self.cur.rowcount, "rows inserted")
        cur = self.con.execute("SELECT * from data_per_pm_id")

        # print column names
        for i in range(len(cur.description)):
            print(cur.description[i][0])
        #for row in cur.fetchall():
        #    print(row["pm_id_0"], row["pm_id_1"], row["pm_id_2"], row["pm_id_3"], row["person_id"], row["belongs_to_datasets"], row["count_belongs_to_datasets"])        

    def self_join_common_pm_ids(self):
        cur = self.con.execute("SELECT A.pm_id_0, A.pm_id_1, A.pm_id_2, A.pm_id_3, B.pm_id_0, B.pm_id_1, B.pm_id_2, B.pm_id_3 from data_per_pm_id as A left join data_per_pm_id as B on A.pm_id_1 = B.pm_id_2 and A.pm_id_0 = B.pm_id_3 UNION " \
            "SELECT A.pm_id_0, A.pm_id_1, A.pm_id_2, A.pm_id_3, B.pm_id_0, B.pm_id_1, B.pm_id_2, B.pm_id_3 from data_per_pm_id as B left join data_per_pm_id as A on A.pm_id_1 = B.pm_id_2 and A.pm_id_0 = B.pm_id_3 ")
        #for row in cur.fetchall():
        #    print(row["pm_id_1"], row["pm_id_2"], row["pm_id_3"], row["pm_id_4"], row["pm_id_1"], row["pm_id_2"], row["pm_id_3"], row["pm_id_4"])

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
        df_res.replace(to_replace=-1, value=nan, inplace=True)
        #df_res2 = deepcopy(df_res)
        #
        # df_res = self.one_merge(df_res, df_res2, "pm_id_new_b", "pm_id_0", "pm_id_3")

        #df_res.replace(to_replace=-1, value=nan, inplace=True)
        # calculate and add columns person_id, belongs_to_datasets, count_belongs_to_datasets

        # save data_per_pm_id.csv
        new_outfile_name, ext = os.path.splitext(self.core["output"]["per_pm_id"][1])
        new_outfile_name = new_outfile_name + "_testy" + ext

        df_res.to_csv(os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))
        print("saved as: ", os.path.join(self.root_data_dir_name, self.core["output"]["per_pm_id"][0], new_outfile_name))


def main(config_filename : str = "config_master_4ds.json", config_path : str = ""):
    acf = add_constraints_filters(config_filename, config_path)
    acf.fill_sql_table()
    acf.self_join_common_pm_ids()
    #acf.join_them()


if __name__ == "__main__":
    fire.Fire(main)
