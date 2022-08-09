#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy
from numpy import nan
import logging
from link_all_datasets import entry_datasets_association, link_all_datasets_pm_id_date

"""
call as: python3 add_constraints_filters.py [--config_filename=IO.json] [--config_path="."]

read the data_per_pm_id.csv and modify it with additional constraints and filters.

the concrete constraints are:
- pm_id_1 (OPENonOH Nightscout Uploader) and pm_id_2 (OPENonOH AAPS Uploader) 
share the same project_member_id, however so far only the info from the BG data is used to identify duplicates.
- same for pm_id_0 (OpenAPS Nightscout Uploader) and pm_id_3 (OpenAPS AAPS Uploader)

recalculate person_id, belongs_to_datasets, count_belongs_to_datasets afterwards.

Perform a outer join of data_per_pm_id.csv with itself based on pm_id_0 == pm_id_3 and pm_id_1 == pm_id_2. 
"""

class add_constraints_filters_per_pm_id():
    def __init__(self, config_filename : str, config_path : str):
        f = open(os.path.join(config_path, config_filename))
        # reading the IO_json config file
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.output = IO_json["link_all_datasets"]["output"]

        # self.output = IO_json["output"]
        self.count_datasets = len(IO_json["link_all_datasets"]["individual"])
        self.IO_json = IO_json


    def merge_per_project(self, df1 : pd.DataFrame, df2 : pd.DataFrame, pm_id_df1, pm_id_df2, suffix_ : str = ""):
        """
        the merge of dataframes belonging to one project (OPENonOH or OpenAPS).
        This function is called twice in create_new_csv_from_self_join() for the two projects.

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
        # these are caught here via an assert and dealt with offline, should they arise.
        for c_1, c_2 in zip(cols_1, cols_2):
            # c_1[:-4] == c_2[:-4]  # just the suffixes _df1 and _df2 are removed
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
            new_outfile_name, ext = os.path.splitext(self.output["per_pm_id"][1])
            new_outfile_name = new_outfile_name + "_" + suffix_ + ext

            df_res.to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], new_outfile_name))
            print("saved as: ", os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], new_outfile_name))


        # now df_res also only has the original pm_id_0 to pm_id_3 columns
        return df_res


    def create_new_csv_from_self_join(self, save : bool = False):
        """
        self outer join the data_per_pm_id.csv
        """
        # load data_per_pm_id.csv
        df1 = pd.read_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], self.output["per_pm_id"][1]))
        df2 = pd.read_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], self.output["per_pm_id"][1]))
        
        df_res1 = self.merge_per_project(df1, df2, "pm_id_1", "pm_id_2")
        df_res2 = self.merge_per_project(df1, df2, "pm_id_0", "pm_id_3")
        df_all = pd.merge(df_res1, df_res2, how="outer", on=["pm_id_1", "pm_id_2", "pm_id_0", "pm_id_3"], suffixes=("_df_res1", "_df_res2"))


        # sort columns
        df_all = df_all[sorted(df_all.columns)]
        assert df_all.drop_duplicates().shape[0] == df_all.shape[0], \
            f"there are duplicate rows in the dataframe: {df_all.drop_duplicates().shape}, {df_all.shape}"

        # calculate and add columns person_id, belongs_to_datasets, count_belongs_to_datasets
        df_all["person_id"] = range(df_all.shape[0])  # here the person_id variable is created. Important!
        entry_datasets_association(df_all)

        if save: 
            orig_name, ext = os.path.splitext(self.output["per_pm_id"][1])
            new_outfile_name = f"{orig_name}_after_constraints_filters{ext}"

            df_all.to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], new_outfile_name))
            print("saved new file as: ", os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], new_outfile_name))

        return df_all


    def fill_project_info(self, df : pd.DataFrame):
        """
        does not use self, except for the path names

        read 'belongs_to_datasets' variable and fill new project_info columns

        convention used here: functions, which are called only by a lambda function are inside this function.
        """
        def project_id(row : list, IDs_to_find : set, join_by=","):
            """
            just used for the lambda-function in df.apply below
            based on the belongs_to_datasets column determine to which project the row belongs to
            """
            assert type(IDs_to_find) == set, f"IDs_to_find must be a set, but is {type(IDs_to_find)}"
            dataset_id_str = str(row["belongs_to_datasets"])
            if ',' in dataset_id_str:
                dataset_ids = dataset_id_str.split(",")
                dataset_ids = [int(x) for x in dataset_ids]
                intersect_ = IDs_to_find & set(dataset_ids)
                if len(intersect_) > 0:
                    return row[f"pm_id_{intersect_.pop()}"]
                else: return nan
            else:
                if int(dataset_id_str) in IDs_to_find:
                    return row[f"pm_id_{dataset_id_str}"]
                else: return nan

        var = "per_pm_id"

        df["project_OPENonOH"] = df.apply(lambda row: project_id(row, {1,2}), axis=1)
        df["project_OpenAPS"] = df.apply(lambda row: project_id(row, {0,3}), axis=1)
        self.entry_projects_association(df)

        df.sort_values(by=["count_belongs_to_projects"], inplace=True)
        df.drop_duplicates([col for col in df.columns if col.startswith("project")], keep="last", inplace=True)  # drop duplicates on pm_id_0 and pm_id_1 is sufficient, since pm_id_0 has been merged with pm_id_3 and pm_id_1 with pm_id_2
        
        fn, ext = os.path.splitext(self.output[var][1])
        new_outfile_name = f"{fn}_with_project_info{ext}"
        df.to_csv(os.path.join(self.root_data_dir_name, self.output[var][0], new_outfile_name))
        print("saved as: ", os.path.join(self.root_data_dir_name, self.output[var][0], new_outfile_name), ", shape: ", df.shape)

    def entry_projects_association(self,df):
        """
        Add two columns to df which describe to which projects a row belongs and to how many different datasets.
        and save it to a csv file

        self is not used, it is just member function of this class, since it is closely related to fill_project_info()
        see also link_all_datasets.entry_datasets_association()
        """
        def belongs_to_project(row : list, column_list : list, join_by=","):
            """
            just used for the lambda-function below
            """
            project_names, project_counter = [], []
            for i,col in enumerate([col for col in column_list if col.startswith("project")]):
                if row[col] is not nan and row[col] > 0:
                    col_id = col.split("_")[-1]  # col: project_OPENonOH, projet_OpenAPS
                    project_names.append(col_id)
                    project_counter.append(f"{i}")
            return join_by.join(project_names), join_by.join(project_counter)

        join_by = ","
        df["belongs_to_project_names"] = df.apply(lambda row: belongs_to_project(row, df.columns)[0], axis=1)
        df["belongs_to_projects"] = df.apply(lambda row: belongs_to_project(row, df.columns)[1], axis=1)
        df["count_belongs_to_projects"] = df.apply(lambda x: len(x["belongs_to_projects"].split(join_by)), axis=1)


def main(config_filename : str = "config_all.json", config_path : str = ""):
    acf = add_constraints_filters_per_pm_id(config_filename, config_path)
    df_pm_id_only = acf.create_new_csv_from_self_join()  
    acf.fill_project_info(df_pm_id_only)

    #lads = link_all_datasets_pm_id_date(config_filename, config_path)
    #lads.set_pm_id_only_table(df_pm_id_only)
    #lads.generate_pm_id_date_table(save = False)  #uses self.out_df_pm_id_only to create pm_id_date_table.csv

if __name__ == "__main__":
    fire.Fire(main)
