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
    logging.info(file_list[:3])  # head 
    logging.info(file_list[-3:])  # and tail
    for i, f in enumerate(file_list):
        one_file(f)

def one_file(filename : str):
    df = pd.read_csv(filename, header=0, parse_dates=[1], index_col=0)
    user_ids_ds1 = set(df["user_id_ds1"].to_numpy().tolist())
    user_ids_ds2 = set(df["user_id_ds2"].to_numpy().tolist())
    filenames_ds1 = set(df["filename_ds1"].to_numpy().tolist())
    filenames_ds2 = set(df["filename_ds2"].to_numpy().tolist())
    print(filename, len(df), user_ids_ds1, user_ids_ds2, filenames_ds1, filenames_ds2)

if __name__ == "__main__":
    main()