#! /usr/bin/env python3

import glob
import logging
import os
import pandas as pd
"""

"""

def config_logger(console_log_level=logging.INFO):
    logging.basicConfig(filename="process_test_datasets.log", level=logging.DEBUG)
    logger = logging.getLogger("")
    # Set up logging to the console.
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(console_log_level)
    logger.addHandler(stream_handler)


def main():
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

def create_filenames_set(filenames_set : set):
    new_filenames_set = {}
    for fn in sorted(list(filenames_set)):
        comps = fn.split("_")
        user_id = comps[0]
        try:
            user_id = int(user_id)
        except ValueError:
            return {}

        fn_new = f"{user_id}_entries_file.csv"
        i = 0
        while fn_new in new_filenames_set:
            fn_new = f"{user_id}_entries_file{i}.csv"
            i += 1
        new_filenames_set.update(fn_new)
    return new_filenames_set

def modify_one_duplicates_file(filename : str):
    df = pd.read_csv(filename, header=0, parse_dates=[1], index_col=0)
    df["user_id_ds1"] = df["user_id_ds1"].astype(float)
    df["user_id_ds1"] = df["user_id_ds1"].apply(lambda x: int(x/1000)*1000)
    df["user_id_ds2"] = df["user_id_ds2"].astype(float)
    df["user_id_ds2"] = df["user_id_ds2"].apply(lambda x: int(x/1000)*1000)
    df["filename_ds1"] = df["user_id_ds1"].apply(lambda x: f"{x}_entries_file.csv")
    df["filename_ds2"] = df["user_id_ds2"].apply(lambda x: f"{x}_entries_file.csv")
    df.to_csv(filename)

if __name__ == "__main__":
    main()