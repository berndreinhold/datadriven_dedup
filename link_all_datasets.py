#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
import pandasgui as pdg
from numpy import nan

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
        self.output = IO_json["link_all_datasets"]["output"]
        #self.dataset = ["ds1", "duplicates", "ds2"]  # the sequence is e.g. ["OpenAPS", "duplicates", "OPENonOH"], so that the duplicates are drawn between the two datasets

        self.df = {}  # dictionary of all the input dataframes
        self.df_user_id_only = {}  # dictionary of all the input dataframes, but the user_ids only
        self.init_individual_datasets()
        self.init_duplicate_datasets()
        for key in self.df:
            #print(self.df[key])
            print(key, len(self.df[key]))

    def init_individual_datasets(self):
        """
        fill the dictionary of dataframes self.df, read from the csv files
        """
        for ds in self.input:
            if "comment" in ds: continue
            if "duplicate" in ds: continue
            infile = self.input[ds]
            self.df[ds] = pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            self.df[ds][f"user_id_{infile[3]}"] = self.df[ds]["user_id"].astype(int)
            self.df[ds] = self.df[ds][["date", f"user_id_{infile[3]}"]]
            self.df[ds][f"label_{infile[3]}"] = infile[2]  # dataset or file label
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            self.df_user_id_only[ds]=self.df[ds][[f"user_id_{infile[3]}", f"label_{infile[3]}"]].drop_duplicates()
            self.df_user_id_only[ds]=self.df_user_id_only[ds][f"user_id_{infile[3]}"]
            #self.df_user_id_only[ds]=self.df[ds][f"user_id_{infile[3]}"].unique()

        self.out_df = pd.DataFrame()  # make the dataframe "per user_id" available throughout the class

    def init_duplicate_datasets(self):
        
        for ds in self.input:
            if "comment" in ds: continue
            if ds.startswith("ds"): continue
            infile = self.input[ds]
            self.df[ds] = pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            id_dupl = self.input[ds][3]  # e.g. "1-2", see config*.json-files
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
        dfs, dfs_duplicates = [], []
        dfs_merged = {}  # dictionary of merged data frames

        for key in sorted(self.df_user_id_only):
            if not key.startswith("ds"): continue
            input_ID = int(self.input[key][3])  # e.g. "1", see config*.json-files
        
            dfs_merged[input_ID] = {}
            dupl_ids = []
            for key_dupl in sorted(self.df_user_id_only):
                if not "duplicates" in key_dupl: continue
                id_dupl = self.input[key_dupl][3]  # e.g. "1-2", see config*.json-files
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
        dfs_merged["second_row"].to_csv(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + ext))
        print(os.path.join(self.root_data_dir_name, self.output["per_user_id"][0], outfilename + ext))

        self.out_df = dfs_merged["second_row"]  # make it available throughout the class

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



    def merge_dataframes_one_column(self, df_merged : pd.DataFrame, col_name : str):
        """
        this must be already the second step.
        col_name is a column which is present in both the left and right dataset and that is not the column on which the join between the two datasets was performed.
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


class legacy():
    def merge_with_duplicates_dataset(self):
        """
        input: the three data frames "OpenAPS", "OPENonOH", "duplicates"

        output and algo: 
        df["OpenAPS_duplicates"]: merge the OpenAPS and the duplicates dataset on [user_id, date]/[user_id_ds1, date] (outer join) 
        df["OPENonOH_duplicates"]: merge the OPENonOH with the duplicates dataset on [user_id, date]/[user_id_ds2, date] (outer join)
        
        add merged data frames OpenAPS_duplicates and OPENonOH_duplicates to self.df-dictionary of data frames
        """
        for ds in ["ds1", "ds2"]:
            self.df[f"{ds}_duplicates"] = self.df[ds].merge(self.df["duplicates"], left_on=["date", "user_id"], right_on=["date", f"user_id_{ds}"], 
                how="outer", suffixes=(f"_{ds}", None))

        # prepare the user_id variables for the merge of ds1_duplicates and ds2_duplicates
        self.df["ds1_duplicates"] = self.df["ds1_duplicates"][["date", "user_id", "user_id_ds2"]]
        self.df["ds1_duplicates"]["user_id_ds1"] = self.df["ds1_duplicates"]["user_id"]
        self.df["ds2_duplicates"] = self.df["ds2_duplicates"][["date", "user_id", "user_id_ds1"]]  
        self.df["ds2_duplicates"]["user_id_ds2"] = self.df["ds2_duplicates"]["user_id"]
        print("ds1-duplicates: ", len(self.df['ds1_duplicates']))
        print("ds2-duplicates: ", len(self.df['ds2_duplicates']))

        # focus just on the date, user_id_* variables, discard e.g. the diff_* variables
        self.df["ds1_duplicates"] = self.df[f"ds1_duplicates"][["date", "user_id_ds1", "user_id_ds2"]]
        self.df["ds2_duplicates"] = self.df[f"ds2_duplicates"][["date", "user_id_ds1", "user_id_ds2"]]  
        
    def merge_all(self):
        """
        input: 
            all the individual dataframes as well as the duplicate frames loaded in the constructor

        output: 
        df["merged_all"] with entries being uniquely identified by date, user_id_ds1, user_id_ds2

        note:
        an assumption here is that the sets of user_id_ds1 and user_id_ds2 are disjunct. Or if they are not disjunct, that they at least refer to the same person.
        If there are instances where user_id_ds1 == user_id_ds2, they will be joined below, even though they might not be in the duplicates list.
        Probably a not very likely risk: See disjunct_user_ids().
        """

        # then merge these on user_id_ds1, user_id_ds2 and date (outer join)
        self.df["merged_all"] = self.df["ds1_duplicates"].merge(self.df["ds2_duplicates"], left_on=["date", "user_id_ds1", "user_id_ds2"], 
            right_on=["date", "user_id_ds1", "user_id_ds2"], how="outer")
        print("merged_all: ", len(self.df["merged_all"]))

        #merged all dataset duplicates:
        outfilename, ext = os.path.splitext(self.IO[self.dataset_pair_index]["duplicates"][1])
        fn_components = outfilename.split("_")
        outfilename = [fn_components[0], "merged_all"]
        outfilename.extend(fn_components[1:])
        outfilename = "_".join(outfilename)
        self.df["merged_all"].to_csv(os.path.join(self.root_data_dir_name, self.IO[self.dataset_pair_index]["duplicates"][0], outfilename + ext))

    def fine_tuning(self):
        """
        preparation of the dataframe for plotting: 
        1. introduce the "dataset" variable for sorting prior to plotting
        2. calculate an arbitrary auto-incremental index to plot instead of user_id_ds1 and user_id_ds2
        """
        out = self.df["merged_all"]  # just an alias for easier readibility below
        out.loc[pd.isnull(out["user_id_ds2"]), "dataset"] = 1  # ds1
        out.loc[pd.isnull(out["user_id_ds1"]), "dataset"] = 2  # ds2
        out.loc[~(pd.isnull(out["user_id_ds2"]) | pd.isnull(out["user_id_ds1"])), "dataset"] = 3  # duplicates
        out["dataset"] = out["dataset"].astype(int)

        df_person_id = self.generate_person_id_table(out)
        #print(df_person_id)
        #pdg.show(df_person_id)

        # merge the df_person_id with the out dataset in two steps, once on the user_id_ds1, then on the user_id_ds2
        merged_part1 = out[out["dataset"]==1].merge(df_person_id, left_on=["user_id_ds1"], right_on=["user_id_ds1"], how="outer", suffixes = (None, "_person_id"))
        merged_part3 = out[out["dataset"]==3].merge(df_person_id, left_on=["user_id_ds1", "user_id_ds2"], right_on=["user_id_ds1", "user_id_ds2"], how="outer", suffixes = (None, "_person_id"))
        merged_part2 = out[out["dataset"]==2].merge(df_person_id, left_on=["user_id_ds2"], right_on=["user_id_ds2"], how="outer", suffixes = (None, "_person_id"))
        self.df["merged_all"] = pd.concat([merged_part1, merged_part2, merged_part3])  # .merge(df_person_id, left_on=["user_id_ds2"], right_on=["user_id_ds2"], how="outer", suffixes = (None, "_person_id"))
        print(self.df["merged_all"])
        self.df["merged_all"].info()

        self.df["merged_all"] = self.df["merged_all"].groupby(["date", "id", "dataset"], as_index=False, dropna=False).agg("count")
        #print(self.df["merged_all"])
        self.df["merged_all"] = self.df["merged_all"][["date", "id", "dataset"]]
        print("merged_all: ", len(self.df["merged_all"]))

    def generate_person_id_table(self, df):
        df_person_id = df[df["dataset"]==3][["dataset", "user_id_ds1", "user_id_ds2"]]
        persons_ds1, persons_ds2 = set(), set()
        if len(df_person_id) > 0:
            print(df_person_id)
            df_person_id = df_person_id.sort_values(["user_id_ds1", "user_id_ds2"]).drop_duplicates()
            df_person_id["id"] = range(len(df_person_id))
        
            # get the list of user_id_ds1 and user_id_ds2 that are duplicates.
            # the goal here is to list all the unique persons, whether they have two user_ids from the two datasets or one
            persons_ds1 = df[df.dataset==3]["user_id_ds1"].to_numpy().tolist()
            persons_ds1 = set(persons_ds1)

            persons_ds2 = df[df.dataset==3]["user_id_ds2"].to_numpy().tolist()
            persons_ds2 = set(persons_ds2)

        data = []
        # now check all days not in the duplicates dataset whether they are associated with persons already in the duplicates dataset.
        for person_ds1 in df[df.dataset==1]["user_id_ds1"].to_numpy().tolist():
            if person_ds1 not in persons_ds1: 
                persons_ds1.add(person_ds1)
                data.append([len(df_person_id) + len(data), 1, person_ds1, nan])
        
        for person_ds2 in df[df.dataset==2]["user_id_ds2"].to_numpy().tolist():
            if person_ds2 not in persons_ds2: 
                persons_ds2.add(person_ds2)
                data.append([len(df_person_id) + len(data), 2, nan, person_ds2])
        if len(df_person_id) > 0:
            df_person_id = pd.concat([df_person_id, pd.DataFrame(data, columns=["id", "dataset", "user_id_ds1", "user_id_ds2"])], axis=0)
        else:
            df_person_id = pd.DataFrame(data, columns=["id", "dataset", "user_id_ds1", "user_id_ds2"])
        
        #person duplicates:
        outfilename, ext = os.path.splitext(self.IO[self.dataset_pair_index]["duplicates"][1])
        fn_components = outfilename.split("_")
        outfilename = [fn_components[0], "person_id"]
        outfilename.extend(fn_components[1:])
        outfilename = "_".join(outfilename)
        df_person_id.to_csv(os.path.join(self.root_data_dir_name, self.IO[self.dataset_pair_index]["duplicates"][0], outfilename + ext))

        return df_person_id

        
    def loop(self):
        """
        produces several plots, where one plot contains ds1, ds2 and their duplicates. They share a common x-axis range.
        """
        for i, one_plot_config in enumerate(self.input):
            #if not i==1: continue
            print(i, one_plot_config)
            self.merge_with_duplicates_dataset()
            self.merge_all()
            self.fine_tuning()


def main(config_filename : str = "IO.json", config_path : str = "."):
    print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    lads = link_all_datasets_user_id_only(config_filename, config_path)
    lads.generate_user_id_only_table()
    #lads.loop()


if __name__ == "__main__":
    fire.Fire(main)
