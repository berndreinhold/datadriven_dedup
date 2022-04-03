#!/usr/bin/env python3
import json
import os
import pandas as pd
import glob
import pandasgui as pdg
import fire


class duplicates_pairwise():

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [user_id, date]
            - OPENonOH with key [user_id, date]
        output:
            - duplicates file containing the duplicates between the two data files with key [user_id_ds1, user_id_ds2, date]
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.IO = IO_json["duplicates_pairwise"]["IO"]
        self.diff_svg_threshold = IO_json["duplicates_pairwise"]["diff_svg_threshold"]

    def init_one_pair(self, i : int):
        self.json_input = self.IO[i]["input"]
        self.json_output = self.IO[i]["output"]

        self.datasets = [x for x in self.json_input.keys() if "comment" not in x]
        self.df = {}
        for ds in self.datasets:
            infile = self.json_input[ds]
            self.df[ds] = pd.read_csv(os.path.join(infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")


    def create_duplicates_file(self, i : int, debug = False) -> pd.DataFrame:
        id0 = self.datasets[0]
        id1 = self.datasets[1]
        out = self.df[id0].merge(self.df[id1], left_on="date", right_on="date", how="inner", suffixes=("_ds1", "_ds2"))

        for col in self.df[id0].columns:
            if not "sgv" in col: continue
            out["diff_" + col] = 2*(out[col + "_ds1"] - out[col + "_ds2"])/(out[col + "_ds1"] + out[col + "_ds2"])

        df2 = out[(out.diff_sgv_mean*out.diff_sgv_mean < self.diff_svg_threshold) & (out.diff_sgv_std*out.diff_sgv_std < self.diff_svg_threshold) & 
            (out.diff_sgv_min*out.diff_sgv_min < self.diff_svg_threshold) & (out.diff_sgv_max*out.diff_sgv_max < self.diff_svg_threshold) & 
            (out.diff_sgv_count*out.diff_sgv_count < self.diff_svg_threshold)]
        df3 = df2[["user_id_ds1", "user_id_ds2", "date", "diff_sgv_mean", "diff_sgv_std", "diff_sgv_min", "diff_sgv_max", "diff_sgv_count", 
            "filename_ds1", "filename_ds2"]].sort_values(by=["user_id_ds1", "user_id_ds2", "date"])

        if debug and len(df3)>0: gui = pdg.show(df3)

        outdir = self.json_output["duplicates"][0]
        outfilename = self.json_output["duplicates"][1]
        df3.to_csv(os.path.join(outdir, outfilename))
        print(os.path.join(outdir, outfilename) + " created")
        return df3

    def validation(self, df : pd.DataFrame):
        # group by statement has to return exactly one line, otherwise the selection criteria on diff_sgv_*-variables need to be tightened.

        df_duplicates_only = df.groupby(["diff_sgv_mean", "diff_sgv_std", "diff_sgv_min", "diff_sgv_max", "diff_sgv_count"]).agg(["count"])
        if len(df_duplicates_only) > 1: 
            columns = []
            for col in df_duplicates_only.columns:
                columns.append(f"{col[0]}_{col[1]}")
            df_duplicates_only.columns = columns
            df_duplicates_only.fillna(value=0, inplace=True)  #
            df_duplicates_only.reset_index(inplace=True)

            print(df_duplicates_only)
            print("all diff*-entries in df_duplicates_only need to be very close to 0.")
            print(type(df_duplicates_only[["diff_sgv_mean", "diff_sgv_std"]]))
            print(type(df_duplicates_only["diff_sgv_mean"]))
            if df_duplicates_only.any() > self.diff_svg_threshold: raise ValueError()

        # get list of duplicates and the count of days
        df3_mod = df[['user_id_ds1', 'user_id_ds2','date']]
        df4 = df3_mod.groupby(['user_id_ds1', 'user_id_ds2']).agg(["count"])

        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns
        df4.fillna(value=0, inplace=True)  #
        df4.reset_index(inplace=True)
        #print(df4[['user_id_ds1', 'user_id_ds2','date_count']])
        print(df4)


    def loop(self):
        for i, one_pair in enumerate(self.IO):
            print(one_pair)
            self.init_one_pair(i)
            df = self.create_duplicates_file(i)
            if len(df) > 0: self.validation(df)



def main(config_filename : str = "IO.json", config_path : str = "."):

    dp = duplicates_pairwise(config_filename, config_path)
    dp.loop()

if __name__ == "__main__":
    fire.Fire(main)
