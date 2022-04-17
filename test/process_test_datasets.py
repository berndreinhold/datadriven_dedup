#! /usr/bin/env python3

import glob
import logging
import os
import pandas as pd
import pandasgui as pdg
"""

"""

def config_logger(console_log_level=logging.INFO):
    logging.basicConfig(filename="process_test_datasets.log", level=logging.DEBUG)
    logger = logging.getLogger("")
    # Set up logging to the console.
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(console_log_level)
    logger.addHandler(stream_handler)


def main_duplicates():
    config_logger()
    in_dir_name = ""
    file_list = sorted(glob.glob(os.path.join(f"{in_dir_name}","**", "duplicates*.csv"), recursive=True))
    logging.info(f"number of files: {len(file_list)}")
    #logging.info(file_list[:3])  # head 
    #logging.info(file_list[-3:])  # and tail
    for i, f in enumerate(file_list):
        out = read_one_duplicates_file(f)
        if out[1] > 0:
            modify_one_duplicates_file(f)
            out = read_one_duplicates_file(f)
            print(*out)

def read_one_duplicates_file(filename : str):
    df = pd.read_csv(filename, header=0, parse_dates=[1], index_col=0)
    user_ids_ds1 = set(df["user_id_ds1"].to_numpy().tolist())
    user_ids_ds2 = set(df["user_id_ds2"].to_numpy().tolist())
    filenames_ds1 = set(df["filename_ds1"].to_numpy().tolist())
    filenames_ds2 = set(df["filename_ds2"].to_numpy().tolist())
    return (filename, len(df), user_ids_ds1, user_ids_ds2, filenames_ds1, filenames_ds2)
    #return len(df)

def modify_one_duplicates_file(filename : str):
    df = pd.read_csv(filename, header=0, parse_dates=[1], index_col=0)
    df["user_id_ds1"] = df["user_id_ds1"].astype(float)
    df["user_id_ds1"] = df["user_id_ds1"].apply(lambda x: int(x/1000)*1000)
    df["user_id_ds2"] = df["user_id_ds2"].astype(float)
    df["user_id_ds2"] = df["user_id_ds2"].apply(lambda x: int(x/1000)*1000)
    df["filename_ds1"] = df["user_id_ds1"].apply(lambda x: f"{x}_entries_file.csv")
    df["filename_ds2"] = df["user_id_ds2"].apply(lambda x: f"{x}_entries_file.csv")
    df.to_csv(filename)

def main_per_day_files():
    config_logger()
    in_dir_name = ""
    file_list = sorted(glob.glob(os.path.join(f"{in_dir_name}","**", "dataset*per_day.csv"), recursive=True))
    logging.info(f"number of files: {len(file_list)}")
    #logging.info(file_list[:3])  # head 
    #logging.info(file_list[-3:])  # and tail
    for i, f in enumerate(file_list):
        out = read_one_per_day_file(f)
        if out[1] > 0:
            modify_one_per_day_file(f)
            out = read_one_per_day_file(f)
        print(out[0], out[1], len(out[2]), len(out[3]), len(out[4]), sorted(out[2]), sorted(out[3]))
        #print(out[0], out[1], len(out[2]), len(out[3]), len(out[4]))

def read_one_per_day_file(filename : str):
    df = pd.read_csv(filename, header=0, parse_dates=[1], index_col=0)
    user_ids = set(df["user_id"].to_numpy().tolist())
    filenames = set(df["filename"].to_numpy().tolist())
    user_ids_of_filenames = set(df["filename"].apply(lambda x: x.split('_')[0]).to_numpy().tolist())
    return (filename, len(df), user_ids, filenames, user_ids_of_filenames)

def modify_one_per_day_file(filename : str):
    df = pd.read_csv(filename, header=0, parse_dates=[1], index_col=0)
    df["user_id"] = df["user_id"].astype(float)
    df["user_id"] = df["user_id"].apply(lambda x: int(x/1000)*1000)
    df["filename"] = df["user_id"].apply(lambda x: f"{x}_entries_file.csv")
    df["filename"] = df["user_id"].apply(lambda x: f"{x}_entries_file.csv")
    df.to_csv(filename)

def main():
    df1 = pd.read_csv("data/merge_test_case.csv", header=0, parse_dates=[1], index_col=None)
    df2 = pd.read_csv("data/merge_test_case_conflict.csv", header=0, parse_dates=[1], index_col=None)

    df_merged = pd.merge(df1, df2, how="outer", left_on="user_id_ds2", right_on="user_id_ds2")
    
    def get_the_right_value2(row, col_names : str):
        x = row[col_names[0]]
        y = row[col_names[1]]

        # it is clear already from the dataframe selection that one of x or y are not NA
        if pd.isna(x): return y
        elif pd.isna(y): return x
        else:
            if x == y: return x
            else: raise ValueError(f"{x} or {y} are not identical, even though they should be, if they are both not NA. row: {row}")
            


    # process all columns, that the right table brought in (the suffixed columns).
    # give the suffixed colums their old name and merge them 
    
    #df_merged.loc[(~pd.isna(df_merged["user_id_ds3_x"]) |  ~pd.isna(df_merged["user_id_ds3_y"])), "user_id_ds3"] = df_merged.loc[(~pd.isna(df_merged["user_id_ds3_x"]) |  ~pd.isna(df_merged["user_id_ds3_y"])), ["user_id_ds3_x", "user_id_ds3_y"]].apply(lambda x: get_the_right_value(x[0],x[1]), axis=1)
    df_merged.loc[(~pd.isna(df_merged["user_id_ds3_x"]) |  ~pd.isna(df_merged["user_id_ds3_y"])), "user_id_ds3"] = df_merged.loc[(~pd.isna(df_merged["user_id_ds3_x"]) |  ~pd.isna(df_merged["user_id_ds3_y"]))].apply(lambda row: get_the_right_value2(row, ["user_id_ds3_x", "user_id_ds3_y"]), axis=1)
    pdg.show(df_merged)

    # check for uniqueness
    if not len(df_merged.loc[~pd.isna(df_merged["user_id_ds3"]), "user_id_ds3"]) == len(df_merged.loc[~pd.isna(df_merged["user_id_ds3"]), "user_id_ds3"].unique()):
        count_all = len(df_merged.loc[~pd.isna(df_merged["user_id_ds3"]), "user_id_ds3"])
        count_unique = len(df_merged.loc[~pd.isna(df_merged["user_id_ds3"]), "user_id_ds3"].unique())
        #raise ValueError(f"values are not unique anymore in column 'user_id_ds3': count(all): {count_all}, count(unique): {count_unique}")
        print(f"values are not unique anymore in column 'user_id_ds3': count(all): {count_all}, count(unique): {count_unique}")

    # drop the columns from the merge:
    df_merged = df_merged[["user_id_ds1_x", "user_id_ds1_y", "user_id_ds2", "user_id_ds3"]]

    pdg.show(df_merged)

if __name__ == "__main__":
    #main_per_day_files()
    main()