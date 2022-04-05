#/usr/bin/env python3
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os
import json
import fire
import pandasgui as pdg


"""
call as: python3 duplicates_plot.py [--config_filename=IO.json] [--config_path="."]

takes two dataset files and a duplicates file of these two datasets, which has been produced by other code (aggregation.py and preprocessing.py)
"""



class duplicates_plot():

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [user_id, date]
            - OPENonOH with key [user_id, date]
            - duplicates file containing the duplicates between the two data files with key [user_id_ds1, user_id_ds2, date]
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.duplicates_json = IO_json["duplicates_plot"]
        self.IO = IO_json["duplicates_plot"]["IO"]
        self.dataset = ["ds1", "duplicates", "ds2"]  # the sequence is e.g. ["OpenAPS", "duplicates", "OPENonOH"], so that the duplicates are drawn between the two datasets

        # common min and max dates (on the x-axis) across all plots
        self.min_date = pd.to_datetime("2100-12-31",format="%Y-%m-%d")
        self.max_date = pd.to_datetime("1970-01-01",format="%Y-%m-%d")


    def init_one_pair(self, pair_i : int):
        self.df = {}
        for ds in self.dataset:
            infile = self.IO[pair_i][ds]
            self.df[ds] = pd.read_csv(os.path.join(infile[0], infile[1]), header=0, parse_dates=[1], index_col=0)
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            # fix the data types that were loaded as the unspecific "object"
            for col in self.df[ds].columns:
                if "filename" in col:
                    self.df[ds][col] = self.df[ds][col].astype('string')
            if self.df[ds]["date"].min() is not pd.NaT:
                self.min_date = min([self.df[ds]["date"].min(), self.min_date])
            if self.df[ds]["date"].max() is not pd.NaT:
                self.max_date = max([self.df[ds]["date"].max(), self.max_date])
        # fix the data types that were loaded as the unspecific "object"
        self.df["duplicates"]['user_id_ds1'] = self.df["duplicates"]['user_id_ds1'].astype(int)
        
        

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

        # focus just on the date, user_id_* variables
        self.df["ds1_duplicates"] = self.df[f"ds1_duplicates"][["date", "user_id_ds1", "user_id_ds2"]]
        self.df["ds2_duplicates"] = self.df[f"ds2_duplicates"][["date", "user_id_ds1", "user_id_ds2"]]  
        
    def merge_all(self):
        """
        input: 
        the merged data frames ds1_duplicates and ds2_duplicates

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


    def fine_tuning(self):
        """
        preparation of the dataframe for plotting: 
        1. introduce the "dataset" variable for sorting prior to plotting
        2. calculate an arbitrary auto-incremental index to plot instead of user_id_ds1 and user_id_ds2
        """
        out = self.df["merged_all"]  # just an alias for easier readibility below

        out.loc[pd.isnull(out["user_id_ds2"]), "dataset"] = 1  # ds1
        out.loc[pd.isnull(out["user_id_ds1"]), "dataset"] = 3  # ds2
        out.loc[~(pd.isnull(out["user_id_ds2"]) | pd.isnull(out["user_id_ds1"])), "dataset"] = 2  # duplicates
        out["dataset"] = out["dataset"].astype(int)

        out = out.sort_values(by=["dataset", "user_id_ds1", "user_id_ds2", "date"])
        
        # determine an auto-incremental index "id" for plotting instead of the user_id_ds1, user_id_ds2
        df_user_id = out[["dataset", "user_id_ds1", "user_id_ds2"]].groupby(["user_id_ds1", "user_id_ds2"], as_index=False, dropna=False).agg("count")    
        df_user_id = df_user_id.sort_values(["user_id_ds1", "user_id_ds2"])
        df_user_id["id"] = range(len(df_user_id))
        
        df_user_id = df_user_id[["id", "user_id_ds1", "user_id_ds2"]]  # drop the dataset variable, which was only necessary for proper sorting of values prior to calculating the "id"
        
        # merge the df_user_id with the out dataset
        self.df["merged_all"] = out.merge(df_user_id, left_on=["user_id_ds1", "user_id_ds2"], right_on=["user_id_ds1", "user_id_ds2"], how="outer")
        #print(self.df["merged_all"])
        self.df["merged_all"] = self.df["merged_all"].groupby(["date", "id", "dataset"], as_index=False, dropna=False).agg("count")
        self.df["merged_all"] = self.df["merged_all"][["date", "id", "dataset"]]
        #print("merged_all: ", len(self.df["merged_all"]))

    def plot(self, pair_i):
        """
        plot the three data frames: the first, the duplicates and the second dataset.
        store it as output png, where the path and png name is from the config file
        """
        plt.figure(figsize=(9.6, 7.2))
        fig, ax = plt.subplots()

        # affects the sequence in the legend and which one is drawn on top of each other
        for i in range(len(self.dataset), 0, -1):
            self.plot_one_dataset(ax, i, pair_i)

        # x-axis date formatting
        plt.xlim(self.min_date - pd.Timedelta(days = 100), self.max_date + pd.Timedelta(days = 100))  # 100 days as a margin for plotting
        
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
        plt.gcf().autofmt_xdate()
        plt.grid()

        plt.title(f"""{self.duplicates_json["plot"]["title_prefix"]} ({self.IO[pair_i]["ds1"][2]}, {self.IO[pair_i]["ds2"][2]})""")
        plt.xlabel("date")
        plt.ylabel("person (index)")
        plt.setp( plt.gca().get_xticklabels(),  rotation            = 30,
                                            horizontalalignment = 'right'
                                            )
        plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
        plt.tight_layout()

        print("saved figure: ", os.path.join(self.IO[pair_i]["img_path"][0], self.IO[pair_i]["img_path"][1]))
        plt.savefig(os.path.join(self.IO[pair_i]["img_path"][0], self.IO[pair_i]["img_path"][1]), )


    def plot_one_dataset(self, ax, dataset, pair_i):
        # the data
        df = self.df["merged_all"] 
        x = df.loc[df["dataset"]==dataset, "date"].values
        y = df.loc[df["dataset"]==dataset, "id"].values

        colors = self.duplicates_json["plot"]["colors"]
        #c = [colors[f"{ds}"] for ds in df["dataset"].astype(int).values]
        
        label_ = self.IO[pair_i][self.dataset[dataset-1]][2]  # the key provided to json_input is one of "ds1", "duplicates", "ds2"
        ax.scatter(x,y, marker='s', s=1, c=colors[label_], label=f"{label_} ({len(x)} d)")
        
    def loop(self):
        for i, one_plot_config in enumerate(self.IO):
            self.init_one_pair(i)  # determine min-, max-date across all datasets

        for i, one_plot_config in enumerate(self.IO):
            # print(i, one_plot_config)
            self.init_one_pair(i)
            self.merge_with_duplicates_dataset()
            self.merge_all()
            self.fine_tuning()
            self.plot(i)


def main(config_filename : str = "IO.json", config_path : str = "."):

    dp = duplicates_plot(config_filename, config_path)
    dp.loop()


if __name__ == "__main__":
    fire.Fire(main)
