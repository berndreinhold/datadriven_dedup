#!/usr/bin/env python3
import json
import os
import pandas as pd
import glob
import fire


class duplicatesPairwise():

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [pm_id, date]
            - OPENonOH with key [pm_id, date]
        output:
            - duplicates file containing the duplicates between the two data files with key [pm_id_1, pm_id_2, date] (e.g.)
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.IO = IO_json["duplicates_pairwise"]["IO"]
        self.diff_svg_threshold = float(IO_json["duplicates_pairwise"]["diff_svg_threshold"][0])
        self.root_data_dir_name = IO_json["root_data_dir_name"]


    def init_one_pair(self, i : int):
        self.json_input = self.IO[i]["input"]
        self.json_output = self.IO[i]["output"]

        self.df = []
        infile = self.json_input[0]
        self.df.append(pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0))
        self.df[0]["date"] = pd.to_datetime(self.df[0]["date"],format="%Y-%m-%d")

        infile = self.json_input[1]
        self.df.append(pd.read_csv(os.path.join(self.root_data_dir_name, infile[0], infile[1]), header=0, parse_dates=[1], index_col=0))
        self.df[1]["date"] = pd.to_datetime(self.df[1]["date"],format="%Y-%m-%d")

    def create_duplicates_file(self, i : int, debug = False) -> pd.DataFrame:
        # load correct IO variables
        self.json_input = self.IO[i]["input"]
        self.json_output = self.IO[i]["output"]
        id_dupl = self.json_output[4]
        id_dupl = id_dupl.split("-")
        id = ["_" + id_dupl[0], "_" + id_dupl[1]]

        out = self.df[0].merge(self.df[1], left_on="date", right_on="date", how="inner", suffixes=(id[0], id[1]))

        for col in self.df[0].columns:
            if not "sgv" in col: continue  # only columns containing "sgv": sgv: single glucose value (?)
            out["diff_" + col] = 2*(out[col + id[0]] - out[col + id[1]])/(out[col + id[0]] + out[col + id[1]])

        df2 = out[(out.diff_sgv_mean*out.diff_sgv_mean < self.diff_svg_threshold) & (out.diff_sgv_std*out.diff_sgv_std < self.diff_svg_threshold) & 
            (out.diff_sgv_min*out.diff_sgv_min < self.diff_svg_threshold) & (out.diff_sgv_max*out.diff_sgv_max < self.diff_svg_threshold) & 
            (out.diff_sgv_count*out.diff_sgv_count < self.diff_svg_threshold)]
        df3 = df2[[f"pm_id{id[0]}", f"pm_id{id[1]}", "date", "diff_sgv_mean", "diff_sgv_std", "diff_sgv_min", "diff_sgv_max", "diff_sgv_count", 
            f"filename{id[0]}", f"filename{id[1]}"]].sort_values(by=[f"pm_id{id[0]}", f"pm_id{id[1]}", "date"])

        df3.to_csv(os.path.join(self.root_data_dir_name, self.json_output[0], self.json_output[1]))
        print(os.path.join(self.root_data_dir_name, self.json_output[0], self.json_output[1]) + " created")
        return df3

    def validation(self, i : int, df : pd.DataFrame):
        """
        group by statement involving all diff_sgv-variables has to return exactly one line, 
        otherwise the selection criteria on diff_sgv_*-variables (the diff_sgv_threshold) need to be tightened.
        """

        # load correct IO variables
        self.json_input = self.IO[i]["input"]
        self.json_output = self.IO[i]["output"]
        id_dupl = self.json_output[4]
        id_dupl = id_dupl.split("-")
        id = ["_" + id_dupl[0], "_" + id_dupl[1]]


        df_duplicates_only = df.groupby(["diff_sgv_mean", "diff_sgv_std", "diff_sgv_min", "diff_sgv_max", "diff_sgv_count"]).agg(["count"])
        df2 = df_duplicates_only  # just a bit more compact 
        if len(df2) > 1: 
            columns = []
            for col in df2.columns:
                columns.append(f"{col[0]}_{col[1]}")
            df2.columns = columns
            df2.fillna(value=0, inplace=True)  #
            df2.reset_index(inplace=True)

            print(df2)
            print("all diff*-entries in df2 need to be very close to 0.")
            for col in df2.columns:
                if "diff" not in col: continue
                if (df2[col]*df2[col] > self.diff_svg_threshold).any(): 
                    print(df2[col])
                    print(df2[col]*df2[col] > self.diff_svg_threshold)
                    raise ValueError(f"{col}^2 should be smaller than diff_svg_threshold ({self.diff_svg_threshold})")


        # get list of duplicates and the count of days
        df3_mod = df[[f"pm_id{id[0]}", f"pm_id{id[1]}",'date']]
        df4 = df3_mod.groupby([f"pm_id{id[0]}", f"pm_id{id[1]}"]).agg(["count"])

        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns
        df4.fillna(value=0, inplace=True)  #
        df4.reset_index(inplace=True)
        #print(df4[[f"pm_id{id[0]}", f"pm_id{id[1]}",'date_count']])
        print(df4)


    def loop(self):
        for i, one_pair in enumerate(self.IO):
            print(one_pair)
            self.init_one_pair(i)
            df = self.create_duplicates_file(i)
            if len(df) > 0: self.validation(i, df)



def main(config_filename : str = "config_pairwise.json", config_path : str = "."):

    dp = duplicatesPairwise(config_filename, config_path)
    dp.loop()

if __name__ == "__main__":
    fire.Fire(main)
