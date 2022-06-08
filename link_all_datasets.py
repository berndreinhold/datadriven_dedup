#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
import pandasgui as pdg
from numpy import nan
import re

"""
call as: python3 link_all_datasets.py [--config_filename=IO.json] [--config_path="."]

Create two tables linking all datasets: one with one entry per (pm_id, date), another with one entry per pm_id.
There is one class to create one table each.
These tables are saved as csv-files (one per table).
These tables are then the only input for plotting (pairwise_plot.py, upsetplot_one_df.ipynb, venn3_upsetplot.ipynb)
"""



class link_all_datasets_pm_id_only():
    """
    input: the per-day csv files as well as the pairwise duplicate files.
    output: a table with person_ids and pm_ids in each dataset/uploader pair (e.g. OpenAPS/nightscout), aligned where the duplicate files indicate matching pm_ids across dataset/uploader pairs.
    """

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of csv files of ind(ividual) datasets and their tables of dup(licates):
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.link_all_datasets = IO_json["link_all_datasets"]
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.input_individual = self.link_all_datasets["individual"]
        self.input_duplicate_list = self.link_all_datasets["duplicate"]  # a list
        # a dict with key="0-1", value=["", "duplicates_ds0_ds1_per_day.csv", "duplicates (dataset 0-dataset 1)", "duplicates_ds0_ds1", "0-1"] 
        self.input_duplicate = {d[4]:d for d in self.input_duplicate_list}  
        self.output = self.link_all_datasets["output"]

        self.df = {"ind" : [], "dupl" : {}}  # individual datasets are organized in a list, the duplicate datasets in a dictionary with keys like '0-1', '0-2', '1-2'
        self.df_pm_id_only = {"ind" : [], "dupl" : {}}  # see comment above
        self.init_individual_datasets()
        self.init_duplicate_datasets()
        for i,ds in enumerate(self.df["ind"]):
            print(i, len(ds))
        for key in self.df["dupl"]:
            print(key, len(self.df["dupl"][key]))

        self.out_df_pm_id_only = pd.DataFrame()  # make the dataframe "per pm_id" available throughout the class

    def init_individual_datasets(self):
        """
        fill the dictionary of dataframes self.df, read from the csv files of ind(ividual) datasets (as opposed to dup(licate) datasets)
        self.input_individual is a list
        """
        for i, ds in enumerate(self.input_individual):
            infile = self.input_individual[i]
            self.df["ind"].append(pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0))
            self.df["ind"][i][f"pm_id_{i}"] = self.df["ind"][i][f"pm_id_{i}"].astype(int)
            self.df["ind"][i] = self.df["ind"][i][["date", f"pm_id_{i}"]]
            self.df["ind"][i][f"label_{i}"] = infile[3]  # machine readable label
            self.df["ind"][i]["date"] = pd.to_datetime(self.df["ind"][i]["date"],format="%Y-%m-%d")
            self.df_pm_id_only["ind"].append(self.df["ind"][i][[f"pm_id_{i}", f"label_{i}"]].drop_duplicates())
            self.df_pm_id_only["ind"][i]=self.df_pm_id_only["ind"][i][f"pm_id_{i}"]
            #self.df_pm_id_only[ds]=self.df[ds][f"pm_id_{label_}"].unique()


    def init_duplicate_datasets(self):
        """
        fill the dictionary of dataframes self.df, read from the csv files of dupl(icate) datasets
        self.input_duplicate_list is e.g. 
        """
        for id_dupl in self.input_duplicate:  # id_dupl: e.g. "1-2", see config_all.json-files
            infile = self.input_duplicate[id_dupl]
            self.df["dupl"][id_dupl] = pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            first_ds, second_ds = id_dupl.split("-")  # id_dupl = "1-2"
            first_ds, second_ds = int(first_ds), int(second_ds)

            self.df["dupl"][id_dupl][f'pm_id_{first_ds}'] = self.df["dupl"][id_dupl][f'pm_id_{first_ds}'].astype(int)
            self.df["dupl"][id_dupl][f'pm_id_{second_ds}'] = self.df["dupl"][id_dupl][f'pm_id_{second_ds}'].astype(int)
            self.df["dupl"][id_dupl] = self.df["dupl"][id_dupl][["date", f"pm_id_{first_ds}", f"pm_id_{second_ds}"]]
            #self.df["dupl"][id_dupl][f"label_{id_dupl}"] = infile[2]  # dataset or file label
            self.df["dupl"][id_dupl]["date"] = pd.to_datetime(self.df["dupl"][id_dupl]["date"],format="%Y-%m-%d")
            # fix the data types that were loaded as the unspecific "object" - TODO: is this still an issue 
            # self.df_pm_id_only[id_dupl]=self.df[id_dupl][[f"pm_id_{first_ds}", f"pm_id_{second_ds}", f"label_{id_dupl}"]].drop_duplicates()
            self.df_pm_id_only["dupl"][id_dupl]=self.df["dupl"][id_dupl][[f"pm_id_{first_ds}", f"pm_id_{second_ds}"]].drop_duplicates()

    def use_not_na_value(self, row, col_names : str):
        """
        is called by df.apply(lambda row: use_not_na_value(row, [col_x, col_y])) below.
        The two columns from the two merged table are suffixed by _x and _y by dataframe::merge())
        Use one of the values from the two tables, that is not NA. It is clear already from the dataframe selection that exactly one of x or y are not NA.
        """
        value_x = row[col_names[0]]
        value_y = row[col_names[1]]

        
        if pd.isna(value_x): return value_y
        elif pd.isna(value_y): return value_x
        else:
            if value_x == value_y: return value_x
            else: raise ValueError(f"{value_x} or {value_y} are not identical, even though they should be, if they are both not NA. row:\n{row}")
    

    def generate_pm_id_only_table(self):
        """
        TODO: generalize to 4 files and their duplicates.
        TODO: (optional) generalize to N files and their duplicates.
        """
        dfs, dfs_duplicates = [], []
        dfs_merged = {}  # dictionary of merged data frames: outer join of individual with duplicate dataset

        for key, _ in enumerate(self.df_pm_id_only["ind"]):
        
            dfs_merged[key] = {}
            dupl_ids = []
            for key_dupl in sorted(self.df_pm_id_only["dupl"]):
                first_ds, second_ds = key_dupl.split("-")  # key_dupl = "1-2"
                first_ds, second_ds = int(first_ds), int(second_ds)
                if not int(key) == first_ds and not int(key) == second_ds: continue  # no column match, do nothing
                dupl_ids.append(key_dupl)
                dfs_merged[key][key_dupl] = pd.merge(self.df_pm_id_only["ind"][key], self.df_pm_id_only["dupl"][key_dupl], how="outer", on=f"pm_id_{key}", validate="one_to_one")
                #dfs_merged[(key, key_dupl)] = self.merge_individual_ds_duplicates(self.df_pm_id_only[key], self.df_pm_id_only[key_dupl], f"pm_id_{key}")

            # from here onwards only dfs_merged is being used:
            if int(key)==0: 
                # merge the first row:
                dfs_merged["first_row"] = pd.merge(dfs_merged[key][dupl_ids[0]], dfs_merged[key][dupl_ids[1]], how="outer", on=f"pm_id_{key}", validate="one_to_one")
            elif int(key)==1: 
                #dfs_merged["second_row"] = self.merge_dataframes(dfs_merged["first_row"], dfs_merged[key]["3-2"], join_column= (f"pm_id_{key}",f"pm_id_{key}"))
                dfs_merged["second_row"] = self.merge_dataframes(dfs_merged["first_row"], dfs_merged[key]["1-2"], join_column=f"pm_id_{key}")
            elif int(key)==2: 
                dfs_merged["third_row"] = self.merge_dataframes(dfs_merged["second_row"], dfs_merged[key]["1-2"], join_column=f"pm_id_{key}")


        column_sequence = sorted([x for x in dfs_merged["first_row"].columns if x.startswith("pm_id")])
        dfs_merged["first_row"].sort_values(by=column_sequence, inplace=True)
        
        outfilename, ext = os.path.splitext(self.output["per_pm_id"][1])
        dfs_merged["first_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], outfilename + "_firstrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], outfilename + "_firstrow" + ext))

        column_sequence = sorted([x for x in dfs_merged["second_row"].columns if x.startswith("pm_id")])
        dfs_merged["second_row"].sort_values(by=column_sequence, inplace=True)
        dfs_merged["second_row"]["person_id"] = range(len(dfs_merged["second_row"]))        
        dfs_merged["second_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], outfilename + "_secondrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], outfilename + "_secondrow" + ext))

        column_sequence = sorted([x for x in dfs_merged["third_row"].columns if x.startswith("pm_id")])
        dfs_merged["third_row"].sort_values(by=column_sequence, inplace=True)
        dfs_merged["third_row"]["person_id"] = range(len(dfs_merged["third_row"]))  # here the person_id variable is created. Important!

        self.entry_datasets_association(dfs_merged["third_row"])  # adds two columns to dfs_merged["third_row"]
        self.out_df_pm_id_only = dfs_merged["third_row"]  # make it available throughout the class


    def entry_datasets_association(self, df):
        """
        Add two columns to df which describe to which datasets a row belongs and to how many different datasets.
        """
        def belongs_to_datasets(row : list, column_list : list, join_by=","):
            """
            just used for the lambda-function below
            """
            buffer = []
            for col in column_list:
                if not "pm_id" in col: continue

                if row[col] is not nan and row[col] > 0:
                    col_id = col.split("_")[-1]  # col: pm_id_1
                    buffer.append(col_id)
            return join_by.join(sorted(buffer))

        #dataset-variable: 1-2-3
        #"belongs to" count datasets: 
        df["belongs_to_datasets"] = df.apply(lambda row: belongs_to_datasets(row, df.columns), axis=1)
        df["count_belongs_to_datasets"] = df.apply(lambda x: len(x["belongs_to_datasets"].split(",")), axis=1)

        #pdg.show(df)
        df.to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], self.output["per_pm_id"][1]))
        print(os.path.join(self.root_data_dir_name, self.output["per_pm_id"][0], self.output["per_pm_id"][1]))


    # def merge_individual_ds_duplicates(self, df : pd.DataFrame, df_dupl : pd.DataFrame, join_column : str):
    #     df_merged = pd.merge(df, df_dupl, how="outer", on=join_column)  #e.g. on="pm_id_ds2"
    
    #     # process all columns, that are present in both tables, but not the join_column (these are the suffixed columns).
    #     # transfer the pm_id information from the "column with suffix"-pairs to their columns without suffix
    #     for col in set(df.columns) & set(df_dupl.columns) - set([join_column]):
    #         if col.startswith("label"): continue
    #         self.merge_dataframes_one_column(df_merged, col)

    #     # drop the suffixed columns from the merge, as their content has been transferred to the columns without suffix:
    #     cols = [c for c in df_merged.columns if not c.endswith("_x") and not c.endswith("_y")]
    #     df_merged = df_merged[cols]

    #     return df_merged


    def merge_dataframes(self, df1 : pd.DataFrame, df2 : pd.DataFrame, join_column : str) -> pd.DataFrame:
        df_merged = pd.merge(df1, df2, how="outer", on=join_column)  #e.g. on="pm_id_ds2"
    
        # process all columns, that are present in both tables, but not the join_column (these are the suffixed columns).
        # transfer the pm_id information from the "column with suffix"-pairs to their columns without suffix
        for col in set(df1.columns) & set(df2.columns) - set([join_column]):
            if col.startswith("label"): continue
            self.merge_dataframes_one_column(df_merged, col)

        # drop the suffixed columns from the merge, as their content has been transferred to the columns without suffix:
        cols = [c for c in df_merged.columns if not c.endswith("_x") and not c.endswith("_y")]
        df_merged = df_merged[cols]

        return df_merged



    def merge_dataframes_one_column(self, df_merged : pd.DataFrame, col_name : str):
        """
        Is called by merge_dataframes()
        this must be already the second step.
        col_name is a column which is present in both the left and right dataset and that is not the column on which the join between the two datasets was performed.
        the column is therefore suffixed with pandas default suffices (_x, _y)
        """
        #df_merged.loc[(~pd.isna(df_merged[f"{col_name}_x"]) |  ~pd.isna(df_merged[f"{col_name}_y"])), col_name] = df_merged.loc[(~pd.isna(df_merged[f"{col_name}_x"]) |  ~pd.isna(df_merged[f"{col_name}_y"])), [f"{col_name}_x", f"{col_name}_y"]].apply(lambda x: get_the_right_value(x[0],x[1]), axis=1)
        df_merged.loc[(~pd.isna(df_merged[f"{col_name}_x"]) |  ~pd.isna(df_merged[f"{col_name}_y"])), col_name] = df_merged.loc[(~pd.isna(df_merged[f"{col_name}_x"]) | \
            ~pd.isna(df_merged[f"{col_name}_y"]))].apply(lambda row: self.use_not_na_value(row, [f"{col_name}_x", f"{col_name}_y"]), axis=1)
        #pdg.show(df_merged)

        # check for uniqueness
        if not len(df_merged.loc[~pd.isna(df_merged[col_name]), col_name]) == len(df_merged.loc[~pd.isna(df_merged[col_name]), col_name].unique()):
            count_all = len(df_merged.loc[~pd.isna(df_merged[col_name]), col_name])
            count_unique = len(df_merged.loc[~pd.isna(df_merged[col_name]), col_name].unique())
            #raise ValueError(f"values are not unique anymore in column 'pm_id_ds3': count(all): {count_all}, count(unique): {count_unique}")
            print(f"values are not unique anymore in column '{col_name}': count(all): {count_all}, count(unique): {count_unique}")


    def project_member_id_list_as_filter(self, df : pd.DataFrame, flavor : str ="pm_id"):
        pmid_list = self.link_all_datasets["project_member_id_list"]
        df_pmid = pd.read_csv(os.path.join(self.root_data_dir_name, pmid_list["list"][0], pmid_list["list"][1]))
        df_pmid.columns = ["pm_id"]
        dataset_keys = pmid_list["list"][2]

        df_merge = []  # list of dataframes
        column_list = df.columns
        for i, dataset_key in enumerate(dataset_keys):  # e.g. dataset_key = "2" 
            df_merge.append(pd.merge(df, df_pmid, how="inner", left_on=f"pm_id_{dataset_key}", right_on="pm_id", validate="many_to_one"))
            df_merge[i] = df_merge[i][column_list]

        df2 = pd.concat(df_merge, axis=0)

        buffer = ""
        if flavor == "pm_id_date":
            buffer = os.path.join(self.root_data_dir_name, pmid_list["per_pm_id_date"][0], pmid_list["per_pm_id_date"][1])
        elif flavor == "pm_id":
            buffer = os.path.join(self.root_data_dir_name, pmid_list["per_pm_id"][0], pmid_list["per_pm_id"][1])
        else: 
            raise ValueError(f"unknown flavor ('pm_id' or 'pm_id_date'): {flavor}")

        df2.to_csv(buffer)
        print(f"apply_project_member_id_list(): file created: {buffer}")


class link_all_datasets_pm_id_date(link_all_datasets_pm_id_only):
    def __init__(self, config_filename : str, config_path : str):
        super().__init__(config_filename, config_path)
        self.out_df_pm_id_date = pd.DataFrame()  # make the dataframe "per pm_id_date" available throughout the class, it is the collection of all self.df_pm_id_date-dataframes
        # dictionary of the individual datasets merged with self.out_df_pm_id_only (thereby receiving the person_id-variable)
        self.df_pm_id_date = {"ind" : [], "dupl" : {}}  # individual datasets are organized in a list, the duplicate datasets in a dictionary with keys like '0-1', '0-2', '1-2'
        

    def generate_pm_id_date_table(self, save : bool = False):
        """
        - use the self.out_df_pm_id table to generate the self.out_df_pm_id_date-table
        - add the person_id variable to all the individual datasets and duplicate datasets.
        - concatenate/join/merge them, while avoiding duplicate entries.
        """
        for i in range(len(self.input_individual)):
            self.initial_merge(i, False)
        for ds_key in self.input_duplicate.keys():
            self.initial_merge(ds_key, True)

        dfs, dfs_duplicates = [], []
        dfs_merged = {}  # dictionary of merged data frames

        for key, _ in enumerate(self.df_pm_id_date["ind"]):

            dfs_merged[key] = {}
            dupl_ids = []
            for key_dupl in sorted(self.df_pm_id_date["dupl"]):
                first_ds, second_ds = key_dupl.split("-")  # id_dupl = "1-2"
                first_ds, second_ds = int(first_ds), int(second_ds)
                if not int(key) == first_ds and not int(key) == second_ds: continue  # no column match, do nothing
                dupl_ids.append(key_dupl)
                dfs_merged[key][key_dupl] = pd.merge(self.df_pm_id_date["ind"][key], self.df_pm_id_date["dupl"][key_dupl], how="outer", on=["date", "person_id", f"pm_id_{key}"], validate="one_to_one")
                #dfs_merged[(key, key_dupl)] = self.merge_individual_ds_duplicates(self.df_pm_id_date[key], self.df_pm_id_date[key_dupl], f"pm_id_{key}")

            if int(key)==0: 
                # merge the first row:
                dfs_merged["first_row"] = pd.merge(dfs_merged[key][dupl_ids[0]], dfs_merged[key][dupl_ids[1]], how="outer", on=["date", "person_id", f"pm_id_{key}"], validate="one_to_one")
            elif int(key)==1: 
                #dfs_merged["second_row"] = self.merge_dataframes(dfs_merged["first_row"], dfs_merged[key]["3-2"], join_column= (f"pm_id_{key}",f"pm_id_{key}"))
                print("TODO: fix this! 3-2")
                dfs_merged["second_row"] = self.merge_dataframes_pm_id_date(dfs_merged["first_row"], dfs_merged[key]["1-2"], join_columns=["date", "person_id", f"pm_id_{key}"])
            elif int(key)==2: 
                dfs_merged["third_row"] = self.merge_dataframes_pm_id_date(dfs_merged["second_row"], dfs_merged[key]["1-2"], join_columns=["date", "person_id", f"pm_id_{key}"])

        column_sequence = sorted([x for x in dfs_merged["first_row"].columns if x.startswith("pm_id")])
        dfs_merged["first_row"].sort_values(by=column_sequence, inplace=True)
        
        outfilename, ext = os.path.splitext(self.output["per_pm_id_date"][1])
        dfs_merged["first_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + "_firstrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + "_firstrow" + ext))

        column_sequence = sorted([x for x in dfs_merged["second_row"].columns if x.startswith("pm_id")])
        dfs_merged["second_row"].sort_values(by=column_sequence, inplace=True)
        #dfs_merged["second_row"]["person_id"] = range(len(dfs_merged["second_row"]))
        dfs_merged["second_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + "_secondrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + "_secondrow" + ext))

        column_sequence = sorted([x for x in dfs_merged["third_row"].columns if x.startswith("pm_id")])
        dfs_merged["third_row"].sort_values(by=column_sequence, inplace=True)
        dfs_merged["third_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + "_thirdrow" + ext))
        
        column_sequence = ["person_id"].extend(column_sequence)
        df = pd.merge(dfs_merged["third_row"], self.out_df_pm_id_only, how="left", on=column_sequence, validate="many_to_one")
        
        df.to_csv(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_pm_id_date"][0], outfilename + ext))
        self.out_df_pm_id_date = df # dfs_merged["third_row"]  # make it available throughout the class


    def merge_dataframes_pm_id_date(self, df1 : pd.DataFrame, df2 : pd.DataFrame, join_columns : tuple) -> pd.DataFrame:
        """
        overloads the merge_dataframes()-function of the class link_all_datasets_pm_id_only
        """

        df_merged = pd.merge(df1, df2, how="outer", on=join_columns)  #e.g. on="pm_id_ds2"
    
        # process all columns, that are present in both tables, but not the join_column (these are the suffixed columns).
        # transfer the pm_id information from the "column with suffix"-pairs to their columns without suffix
        for col in set(df1.columns) & set(df2.columns) - set(join_columns):
            if col.startswith("label"): continue
            self.merge_dataframes_one_column(df_merged, col)

        # drop the suffixed columns from the merge, as their content has been transferred to the columns without suffix:
        cols = [c for c in df_merged.columns if not c.endswith("_x") and not c.endswith("_y")]
        df_merged = df_merged[cols]

        return df_merged


    def initial_merge(self, ds_key, dupl : bool):
        """
        does an initial merge of the self.df["dupl"/"ind"][ds_key] with the self.out_df_pm_id_only
        the person_id column is in the self.out_df_pm_id_only-table. A merge with the self.df tables (key: (pm_id, date)) transfers it to self.df_pm_id_date-tables.
        @param ds_key: in case of duplicates a string: key of the ds (short for dataset, e.g. "1-2"), an index in case of individual datasets (datatype: int)
        @param dupl: if True, add the person_id column to the duplicate tables, otherwise to the individual tables
        """
        
        if dupl:  # for duplicates
            if len(self.out_df_pm_id_only) < 1: raise ValueError("self.out_df_pm_id_only dataframe is not filled. Try calling generate_pm_id_only_table() first.")
            assert type(ds_key) == str and ds_key == self.input_duplicate[ds_key][4]  # e.g. "1-2", see config_all.json
            first_ds, second_ds = ds_key.split("-")  # id_dupl = "1-2", 
            first_ds, second_ds = int(first_ds), int(second_ds)
            join_columns = [f"pm_id_{first_ds}", f"pm_id_{second_ds}"]
            self.df_pm_id_date["dupl"][ds_key] = pd.merge(self.df["dupl"][ds_key], self.out_df_pm_id_only, how="left", on=join_columns)  #e.g. on="pm_id_ds2"
            cols = ["date"]
            cols.extend(sorted(join_columns))
            cols.extend(["person_id", "belongs_to_datasets", "count_belongs_to_datasets"])
            self.df_pm_id_date["dupl"][ds_key] = self.df_pm_id_date["dupl"][ds_key][cols]
        else:  # for individual datasets
            assert type(ds_key) == int
            join_column = f"pm_id_{ds_key}"
            self.df_pm_id_date["ind"].append(pd.merge(self.df["ind"][ds_key], self.out_df_pm_id_only, how="left", on=join_column))  #e.g. on="pm_id_ds2"
            cols = ["date"]
            cols.append(join_column)
            cols.extend(["person_id", "belongs_to_datasets", "count_belongs_to_datasets"])
            self.df_pm_id_date["ind"][ds_key] = self.df_pm_id_date["ind"][ds_key][cols]


def main(config_filename : str = "config_all.json", config_path : str = ""):
    lads = link_all_datasets_pm_id_date(config_filename, config_path)
    lads.generate_pm_id_only_table()
    lads.generate_pm_id_date_table(False)
    # a project_member_id_list can be specified in the config_all.json file (config_all.json is created via generate_config_json.py and can then be adjusted by the user)
    #lads.project_member_id_list_as_filter(lads.out_df_pm_id_only, "pm_id")
    #lads.project_member_id_list_as_filter(lads.out_df_pm_id_date, "pm_id_date")

if __name__ == "__main__":
    fire.Fire(main)
