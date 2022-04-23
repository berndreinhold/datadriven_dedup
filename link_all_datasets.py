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

create two tables linking all datasets: one with one entry per (user_id, date), another with one entry per user_id.
these tables are the relevant output of this class: they are saved as csv-files (one per table).
These tables are then the only input for plotting (duplicates_plot.py)
"""



class link_all_datasets_user_id_only():
    """
    input: the per-day csv files as well as the pairwise duplicate files.
    output: a table with person_ids and user_ids in each dataset/uploader pair, aligned where the duplicate files indicate matching user_ids across dataset/uploader pairs.
    """

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [user_id, date]
            - OPENonOH with key [user_id, date]
            - duplicates file containing the duplicates between the two data files with key [user_id_ds1, user_id_ds2, date]
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.duplicates_json = IO_json["link_all_datasets"]
        self.input = IO_json["link_all_datasets"]["input"]
        self.input_individual = self.input["individual"]
        self.input_duplicate = self.input["duplicate"]
        self.output = IO_json["link_all_datasets"]["output"]
        #self.dataset = ["ds1", "duplicates", "ds2"]  # the sequence is e.g. ["OpenAPS", "duplicates", "OPENonOH"], so that the duplicates are drawn between the two datasets

        self.df = {}  # dictionary of all the input dataframes
        self.df_user_id_only = {}  # dictionary of all the input dataframes, but the user_ids only
        self.init_individual_datasets()
        self.init_duplicate_datasets()
        for key in self.df:
            #print(self.df[key])
            print(key, len(self.df[key]))

        self.out_df_user_id_only = pd.DataFrame()  # make the dataframe "per user_id" available throughout the class

    def init_individual_datasets(self):
        """
        fill the dictionary of dataframes self.df, read from the csv files
        """
        for ds in self.input_individual:
            if "comment" in ds: continue
            if "duplicate" in ds or re.match("[0-9]-[0-9]", ds): continue
            infile = self.input_individual[ds]
            self.df[ds] = pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            self.df[ds][f"user_id_{infile[3]}"] = self.df[ds]["user_id"].astype(int)
            self.df[ds] = self.df[ds][["date", f"user_id_{infile[3]}"]]
            self.df[ds][f"label_{infile[3]}"] = infile[2]  # dataset or file label
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            self.df_user_id_only[ds]=self.df[ds][[f"user_id_{infile[3]}", f"label_{infile[3]}"]].drop_duplicates()
            self.df_user_id_only[ds]=self.df_user_id_only[ds][f"user_id_{infile[3]}"]
            #self.df_user_id_only[ds]=self.df[ds][f"user_id_{infile[3]}"].unique()


    def init_duplicate_datasets(self):
        
        for ds in self.input_duplicate:
            if "comment" in ds: continue
            infile = self.input_duplicate[ds]
            self.df[ds] = pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            id_dupl = infile[3]  # e.g. "1-2", see config*.json-files
            first_ds, second_ds = id_dupl.split("-")  # id_dupl = "1-2"
            first_ds, second_ds = int(first_ds), int(second_ds)

            self.df[ds][f'user_id_{first_ds}'] = self.df[ds]['user_id_ds1'].astype(int)
            self.df[ds][f'user_id_{second_ds}'] = self.df[ds]['user_id_ds2'].astype(int)
            self.df[ds] = self.df[ds][["date", f"user_id_{first_ds}", f"user_id_{second_ds}"]]
            #self.df[ds][f"label_{id_dupl}"] = infile[2]  # dataset or file label
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            # fix the data types that were loaded as the unspecific "object" - TODO: is this still an issue 
            # self.df_user_id_only[ds]=self.df[ds][[f"user_id_{first_ds}", f"user_id_{second_ds}", f"label_{id_dupl}"]].drop_duplicates()
            self.df_user_id_only[ds]=self.df[ds][[f"user_id_{first_ds}", f"user_id_{second_ds}"]].drop_duplicates()

    def use_not_na_value(self, row, col_names : str):
        """
        is called by df.apply(lambda row: use_not_na_value(row, [col_x, col_y])) below.
        The two columns from the two merged table are suffixed by _x and _y by dataframe::merge())
        Use one of the values from the two tables, that is not NA. It is clear already from the dataframe selection that one of x or y are not NA.
        """
        value_x = row[col_names[0]]
        value_y = row[col_names[1]]

        
        if pd.isna(value_x): return value_y
        elif pd.isna(value_y): return value_x
        else:
            if value_x == value_y: return value_x
            else: raise ValueError(f"{value_x} or {value_y} are not identical, even though they should be, if they are both not NA. row:\n{row}")
    

    def generate_user_id_only_table(self):
        """
        TODO: generalize to 4 files and their duplicates.
        TODO: (optional) generalize to N files and their duplicates.
        """
        dfs, dfs_duplicates = [], []
        dfs_merged = {}  # dictionary of merged data frames

        for key in sorted(self.df_user_id_only):
            if re.match("[0-9]-[0-9]", key): continue
            input_ID = int(self.input_individual[key][3])  # e.g. "1", see config*.json-files
        
            dfs_merged[input_ID] = {}
            dupl_ids = []
            for key_dupl in sorted(self.df_user_id_only):
                if len(key_dupl) < 3: continue
                id_dupl = self.input_duplicate[key_dupl][3]  # e.g. "1-2", see config*.json-files
                dupl_ids.append(id_dupl)
                first_ds, second_ds = id_dupl.split("-")  # id_dupl = "1-2"
                first_ds, second_ds = int(first_ds), int(second_ds)
                if not input_ID == first_ds and not input_ID == second_ds: continue  # no column match, do nothing
                dfs_merged[input_ID][id_dupl] = pd.merge(self.df_user_id_only[key], self.df_user_id_only[key_dupl], how="outer", on=f"user_id_{input_ID}", validate="one_to_one")
                #dfs_merged[(key, key_dupl)] = self.merge_individual_ds_duplicates(self.df_user_id_only[key], self.df_user_id_only[key_dupl], f"user_id_{input_ID}")

            if input_ID==1: 
                # merge the first row:
                dfs_merged["first_row"] = pd.merge(dfs_merged[input_ID][dupl_ids[0]], dfs_merged[input_ID][dupl_ids[1]], how="outer", on=f"user_id_{input_ID}", validate="one_to_one")
            elif input_ID==2: 
                #dfs_merged["second_row"] = self.merge_dataframes(dfs_merged["first_row"], dfs_merged[input_ID]["3-2"], join_column= (f"user_id_{input_ID}",f"user_id_{input_ID}"))
                print("TODO: fix this! 3-2")
                dfs_merged["second_row"] = self.merge_dataframes(dfs_merged["first_row"], dfs_merged[input_ID]["3-2"], join_column=f"user_id_{input_ID}")
            elif input_ID==3: 
                dfs_merged["third_row"] = self.merge_dataframes(dfs_merged["second_row"], dfs_merged[input_ID]["3-2"], join_column=f"user_id_{input_ID}")


        #for key in dfs_merged:
        #    print(key)
        #    pdg.show(dfs_merged[key])
        dfs_merged["first_row"] = dfs_merged["first_row"].sort_values(by=["user_id_1", "user_id_2", "user_id_3"])
        #pdg.show(dfs_merged["first_row"])
        outfilename, ext = os.path.splitext(self.output["per_user_id"][1])
        dfs_merged["first_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + "_firstrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + "_firstrow" + ext))

        dfs_merged["second_row"] = dfs_merged["second_row"].sort_values(by=["user_id_1", "user_id_2", "user_id_3"])
        dfs_merged["second_row"]["person_id"] = range(len(dfs_merged["second_row"]))
        #pdg.show(dfs_merged["second_row"])
        dfs_merged["second_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + "_secondrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + "_secondrow" + ext))

        dfs_merged["third_row"] = dfs_merged["third_row"].sort_values(by=["user_id_1", "user_id_2", "user_id_3"])
        dfs_merged["third_row"]["person_id"] = range(len(dfs_merged["third_row"]))
        #pdg.show(dfs_merged["third_row"])
        dfs_merged["third_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + ext))
        self.out_df_user_id_only = dfs_merged["third_row"]  # make it available throughout the class


    # def merge_individual_ds_duplicates(self, df : pd.DataFrame, df_dupl : pd.DataFrame, join_column : str):
    #     df_merged = pd.merge(df, df_dupl, how="outer", on=join_column)  #e.g. on="user_id_ds2"
    
    #     # process all columns, that are present in both tables, but not the join_column (these are the suffixed columns).
    #     # transfer the user_id information from the "column with suffix"-pairs to their columns without suffix
    #     for col in set(df.columns) & set(df_dupl.columns) - set([join_column]):
    #         if col.startswith("label"): continue
    #         self.merge_dataframes_one_column(df_merged, col)

    #     # drop the suffixed columns from the merge, as their content has been transferred to the columns without suffix:
    #     cols = [c for c in df_merged.columns if not c.endswith("_x") and not c.endswith("_y")]
    #     df_merged = df_merged[cols]

    #     return df_merged


    def merge_dataframes(self, df1 : pd.DataFrame, df2 : pd.DataFrame, join_column : str) -> pd.DataFrame:
        df_merged = pd.merge(df1, df2, how="outer", on=join_column)  #e.g. on="user_id_ds2"
    
        # process all columns, that are present in both tables, but not the join_column (these are the suffixed columns).
        # transfer the user_id information from the "column with suffix"-pairs to their columns without suffix
        for col in set(df1.columns) & set(df2.columns) - set([join_column]):
            if col.startswith("label"): continue
            self.merge_dataframes_one_column(df_merged, col)

        # drop the suffixed columns from the merge, as their content has been transferred to the columns without suffix:
        cols = [c for c in df_merged.columns if not c.endswith("_x") and not c.endswith("_y")]
        df_merged = df_merged[cols]

        return df_merged

    def merge_dataframes2(self, df1 : pd.DataFrame, df2 : pd.DataFrame, join_columns : tuple) -> pd.DataFrame:
        # TODO: some form of decorator to mimick function overloading would be nicer
        # TODO: merge expressive function name
        df_merged = pd.merge(df1, df2, how="outer", on=join_columns)  #e.g. on="user_id_ds2"
    
        # process all columns, that are present in both tables, but not the join_column (these are the suffixed columns).
        # transfer the user_id information from the "column with suffix"-pairs to their columns without suffix
        for col in set(df1.columns) & set(df2.columns) - set(join_columns):
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
            #raise ValueError(f"values are not unique anymore in column 'user_id_ds3': count(all): {count_all}, count(unique): {count_unique}")
            print(f"values are not unique anymore in column '{col_name}': count(all): {count_all}, count(unique): {count_unique}")

class link_all_datasets_user_id_date(link_all_datasets_user_id_only):
    def __init__(self, config_filename : str, config_path : str):
        super().__init__(config_filename, config_path)
        self.out_df_user_id_date = pd.DataFrame()  # make the dataframe "per user_id_date" available throughout the class, it is the collection of all self.df_user_id_date-dataframes
        self.df_user_id_date = {}  # dictionary of the individual datasets merged with self.out_df_user_id_only (thereby receiving the person_id-variable)

    def generate_user_id_date_table(self, save : bool = False):
        """
        - use the self.out_df_user_id table to generate the self.out_df_user_id_date-table
        - add the person_id variable to all the individual datasets and duplicate datasets.
        - concatenate/join/merge them, while avoiding duplicate entries.
        """
        all_keys = list(self.input_individual.keys())
        all_keys.extend(self.input_duplicate.keys())
        for ds in all_keys:
            if "comment" in ds: continue
            self.add_person_id_2_user_id_only_tables(ds)
            if save: 
                self.df_user_id_date[ds].to_csv(f"test_{ds}.csv")
                print(f"output file created: test_{ds}.csv")


        dfs, dfs_duplicates = [], []
        dfs_merged = {}  # dictionary of merged data frames

        for key in sorted(self.df_user_id_date):
            if re.match("[0-9]-[0-9]", key): continue  # there is a limit on 10x10 datasets to be linked because of this regular expression. "10-1" would not match this reg exp
            input_ID = int(self.input_individual[key][3])  # e.g. "1", see config*.json-files
        
            dfs_merged[input_ID] = {}
            dupl_ids = []
            for key_dupl in sorted(self.df_user_id_date):
                if len(key_dupl) < 3: continue
                id_dupl = self.input_duplicate[key_dupl][3]  # e.g. "1-2", see config*.json-files
                dupl_ids.append(id_dupl)
                first_ds, second_ds = id_dupl.split("-")  # id_dupl = "1-2"
                first_ds, second_ds = int(first_ds), int(second_ds)
                if not input_ID == first_ds and not input_ID == second_ds: continue  # no column match, do nothing
                dfs_merged[input_ID][id_dupl] = pd.merge(self.df_user_id_date[key], self.df_user_id_date[key_dupl], how="outer", on=["date", "person_id", f"user_id_{input_ID}"], validate="one_to_one")
                #dfs_merged[(key, key_dupl)] = self.merge_individual_ds_duplicates(self.df_user_id_date[key], self.df_user_id_date[key_dupl], f"user_id_{input_ID}")

            if input_ID==1: 
                # merge the first row:
                dfs_merged["first_row"] = pd.merge(dfs_merged[input_ID][dupl_ids[0]], dfs_merged[input_ID][dupl_ids[1]], how="outer", on=["date", "person_id", f"user_id_{input_ID}"], validate="one_to_one")
            elif input_ID==2: 
                #dfs_merged["second_row"] = self.merge_dataframes(dfs_merged["first_row"], dfs_merged[input_ID]["3-2"], join_column= (f"user_id_{input_ID}",f"user_id_{input_ID}"))
                print("TODO: fix this! 3-2")
                dfs_merged["second_row"] = self.merge_dataframes2(dfs_merged["first_row"], dfs_merged[input_ID]["3-2"], join_columns=["date", "person_id", f"user_id_{input_ID}"])
            elif input_ID==3: 
                dfs_merged["third_row"] = self.merge_dataframes2(dfs_merged["second_row"], dfs_merged[input_ID]["3-2"], join_columns=["date", "person_id", f"user_id_{input_ID}"])


        #for key in dfs_merged:
        #    print(key)
        #    pdg.show(dfs_merged[key])
        dfs_merged["first_row"] = dfs_merged["first_row"].sort_values(by=["user_id_1", "user_id_2", "user_id_3"])
        #pdg.show(dfs_merged["first_row"])
        outfilename, ext = os.path.splitext(self.output["per_user_id_date"][1])
        dfs_merged["first_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id_date"][0], outfilename + "_firstrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id_date"][0], outfilename + "_firstrow" + ext))

        dfs_merged["second_row"] = dfs_merged["second_row"].sort_values(by=["user_id_1", "user_id_2", "user_id_3"])
        dfs_merged["second_row"]["person_id"] = range(len(dfs_merged["second_row"]))
        #pdg.show(dfs_merged["second_row"])
        dfs_merged["second_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id_date"][0], outfilename + "_secondrow" + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id_date"][0], outfilename + "_secondrow" + ext))

        dfs_merged["third_row"] = dfs_merged["third_row"].sort_values(by=["user_id_1", "user_id_2", "user_id_3"])
        #dfs_merged["third_row"]["person_id"] = range(len(dfs_merged["third_row"]))
        #pdg.show(dfs_merged["third_row"])
        dfs_merged["third_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id_date"][0], outfilename + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id_date"][0], outfilename + ext))
        self.out_df_user_id_date = dfs_merged["third_row"]  # make it available throughout the class


    def add_person_id_2_user_id_only_tables(self, ds):
        if len(self.out_df_user_id_only) < 1: raise ValueError("self.out_df_user_id_only table is not filled. Try calling generate_user_id_only_table() first.")
        # for duplicates
        if re.match("[0-9]-[0-9]", ds):
            id_dupl = self.input_duplicate[ds][3]  # e.g. "1-2", see config*.json-files
            first_ds, second_ds = id_dupl.split("-")  # id_dupl = "1-2"
            first_ds, second_ds = int(first_ds), int(second_ds)
            join_columns = [f"user_id_{first_ds}", f"user_id_{second_ds}"]
            self.df_user_id_date[ds] = pd.merge(self.df[ds], self.out_df_user_id_only, how="left", on=join_columns)  #e.g. on="user_id_ds2"
            cols = ["date", "person_id"]
            cols.extend(join_columns)
            self.df_user_id_date[ds] = self.df_user_id_date[ds][cols]
        # for individual datasets:
        elif len(ds) < 3: 
            id_dupl = self.input_individual[ds][3]  # e.g. "1", see config*.json-files
            join_column = f"user_id_{id_dupl}"
            self.df_user_id_date[ds] = pd.merge(self.df[ds], self.out_df_user_id_only, how="left", on=join_column)  #e.g. on="user_id_ds2"
            cols = ["date", "person_id"]
            cols.append(join_column)
            self.df_user_id_date[ds] = self.df_user_id_date[ds][cols]
        else:
            raise KeyError(f"{ds} is an unexpected key.")
        print(self.df_user_id_date[ds])


def main(config_filename : str = "IO.json", config_path : str = "."):
    print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    lads = link_all_datasets_user_id_date(config_filename, config_path)
    lads.generate_user_id_only_table()
    lads.generate_user_id_date_table(False)


if __name__ == "__main__":
    fire.Fire(main)
