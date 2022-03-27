#/usr/bin/env python3
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os
import json
import fire
from preprocessing import datasets

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
        self.json_input = IO_json["duplicates_plot"]["input"]
        self.json_output = IO_json["duplicates_plot"]["output"]

        self.dataset = ["ds1", "duplicates", "ds2"]  # the sequence is ["OpenAPS", "duplicates", "OPENonOH"], so that the duplicates are drawn between the two datasets
        self.df = {}
        for ds in self.dataset:
            self.df[ds] = pd.read_csv(os.path.join(self.json_input[ds][0], self.json_input[ds][1]), header=0, parse_dates=[1], index_col=0)
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            # fix the data types that were loaded as the unspecific "object"
            for col in self.df[ds].columns:
                if "filename" in col:
                    self.df[ds][col] = self.df[ds][col].astype('string')

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
        self.df[f"ds1_duplicates"] = self.df[f"ds1_duplicates"][["date", "user_id", "user_id_ds2"]]
        self.df[f"ds1_duplicates"]["user_id_ds1"] = self.df[f"ds1_duplicates"]["user_id"]
        self.df[f"ds2_duplicates"] = self.df[f"ds2_duplicates"][["date", "user_id", "user_id_ds1"]]  
        self.df[f"ds2_duplicates"]["user_id_ds2"] = self.df[f"ds2_duplicates"]["user_id"]

        # focus just on the date, user_id_* variables
        self.df[f"ds1_duplicates"] = self.df[f"ds1_duplicates"][["date", "user_id_ds1", "user_id_ds2"]]
        self.df[f"ds2_duplicates"] = self.df[f"ds2_duplicates"][["date", "user_id_ds1", "user_id_ds2"]]  
        
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
    
    def plot(self):
        """
        plot the three data frames: the first, the duplicates and the second dataset.
        store it as output png, where the path and png name is from the config file
        """
        plt.figure(figsize=(9.6, 7.2))
        fig, ax = plt.subplots()

        # affects the sequence in the legend
        for i in range(len(self.dataset), 0, -1):
            self.plot_one_dataset(ax, i)

        # x-axis date formatting
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
        plt.gcf().autofmt_xdate()
        plt.grid()

        plt.title(self.json_output["title_prefix"])
        plt.xlabel("date")
        plt.ylabel("person (index)")
        plt.setp( plt.gca().get_xticklabels(),  rotation            = 30,
                                            horizontalalignment = 'right'
                                            )
        plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
        plt.tight_layout()

        print("saved figure: ", os.path.join(self.json_output["img_path"][0], self.json_output["img_path"][1]))
        plt.savefig(os.path.join(self.json_output["img_path"][0], self.json_output["img_path"][1]), )


    def plot_one_dataset(self, ax, dataset):
        # the data
        df = self.df["merged_all"] 
        x = df.loc[df["dataset"]==dataset, "date"].values
        y = df.loc[df["dataset"]==dataset, "id"].values

        colors = self.json_output["colors"]
        #c = [colors[f"{ds}"] for ds in df["dataset"].astype(int).values]
        
        label_ = self.json_input[self.dataset[dataset-1]][2]  # the key provided to json_input is one of "ds1", "duplicates", "ds2"
        ax.scatter(x,y, marker='s', s=1, c=colors[f"{dataset}"], label=f"{label_} ({len(x)} d)")
        
        

def main(config_filename : str = "IO.json", config_path : str = "."):

    dp = duplicates_plot(config_filename, config_path)

    dp.merge_with_duplicates_dataset()
    dp.merge_all()
    dp.fine_tuning()
    dp.plot()


if __name__ == "__main__":
    fire.Fire(main)
