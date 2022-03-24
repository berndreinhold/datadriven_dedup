import json
import os
import pandas as pd
import numpy as np
import glob
import fire

class aggregation(object):

    def __init__(self, config_filename : str, config_path : str, dataset : str = "OpenAPS_NS"):
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.dataset = dataset
        
        assert(self.dataset in self.datasets)

        self.json_input = IO_json["aggregation"][self.dataset]["input"]
        self.json_output = IO_json["aggregation"][self.dataset]["output"]

        self.in_dir_name = self.json_input["dir_name"]
        self.columns = self.json_input["columns"]
        self.file_pattern = self.json_input["file_pattern"]

        self.out_dir_name = self.json_output["dir_name"]
        os.makedirs(self.out_dir_name, exist_ok=True)

def all_csv(indir_name, df_list):
    for i, f in enumerate(glob.glob(os.path.join(f"{indir_name}","**", "*entries*.csv"), recursive=True)):
        head, tail = os.path.split(f)
        print(i, tail)
        # df = pd.read_csv(f, header=0, parse_dates=[4], index_col=0)
        df = pd.read_csv(f, header=0, index_col=0)
        if len(df)<1:
            print(f"no entries: {head}, {tail}") 
            continue
        
        df["unix_timestamp"] = df["date"]
        # unix_timestamp in ms is a 13 digit number, in s it is a 10 digit number (in 2022)
        if not ((np.log10(df["unix_timestamp"]) > 12) & (np.log10(df["unix_timestamp"]) < 13)).all(): 
            raise ValueError("expected a 13 digit unix timestamp, but got a {} digit number.".format(np.int(np.log10(df["unix_timestamp"][0])) + 1))
        #df[["date","time"]] = df["dateString"].str.split("T", 1, expand=True)
        df["datetime_utc"] = pd.to_datetime(df['unix_timestamp'], unit='ms', utc=True)
        df["date"] = df["datetime_utc"].dt.date
        df_mod = df[["date","sgv"]]
        df2 = df_mod.groupby("date", as_index=False).agg(["mean", "std", "min", "max", "count"])
        # df2.reset_index()
        columns = []
        for col in df2.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df2.columns = columns
        df2.fillna(value=0, inplace=True)  #
        df2.reset_index(inplace=True)
        df2["filename"] = tail
        fn_components = tail.split("_")
        if "OPENonOH" in head: 
            df2["user_id"] = fn_components[0]
            df2["second_id"] = fn_components[1]
        elif "OpenAPS" in head:
            df2["user_id"] = fn_components[0]
            df2["second_id"] = np.nan
        else: 
            raise ValueError("dataset needs to be in file path, it should be either 'OPENonOH' or OpenAPS")
        
        df_list.append(df2)
        

def main(dataset : str, config_filename : str = "IO.json", config_path : str = "."):

    indir = f"/home/reinhold/Daten/OPEN/{dataset}_Data/csv_per_measurement"
    outdir = f"/home/reinhold/Daten/OPEN/{dataset}_Data/csv_per_day/"
    outfile_name = f"entries_{dataset}.csv"
    df_list = []
    all_csv(indir, df_list)
    df = pd.concat(df_list, axis=0)
    #print(df)
    #df.info()
    os.makedirs(outdir, exist_ok=True)
    df.to_csv(os.path.join(outdir, outfile_name))
    print(os.path.join(outdir, outfile_name) + " created")


if __name__ == "__main__":
    fire.Fire(main)


