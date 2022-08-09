#/usr/bin/env python3
from operator import index
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os
import json
import fire
from numpy import nan

"""
call as: python3 pairwise_plot.py [--config_filename=config_viz.json] [--config_path="."]
"""



class pairwisePlot():
    """
    one pairwise plot shows dataset e.g. 1, 2 and the duplicates between them on a xy-plot with date on the x-axis and a person counter on the y-axis.
    this class produces all pairwise plots listed in the generated config_viz.json-file read as input. (see generate_config_json.py and config_master*.json)
    """
    def __init__(self, config_filename : str, config_path : str):
        """
        read two (generated) config files: 
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.input = IO_json["pairwise_plots"]["input"]
        self.output = IO_json["pairwise_plots"]["output"]
        self.plot_config = IO_json["pairwise_plots"]["plot_config"]


        # read data file
        self.df = pd.read_csv(os.path.join(self.root_data_dir_name, self.input[0], self.input[1]), header=0, parse_dates=[1], index_col=0)
        self.df["date"] = pd.to_datetime(self.df["date"],format="%Y-%m-%d")

        # common min and max dates (on the x-axis) across all plots
        self.min_date = self.df["date"].min() 
        self.max_date = self.df["date"].max()
        self.min_person_id = self.df["person_id"].min() 
        self.max_person_id = self.df["person_id"].max()

    def plot(self, pair_i):
        """
        plot the three data frames: the first, the duplicates and the second dataset.
        store it as output png, where the path and png name is from the config file
        """
        plt.figure(figsize=(9.6, 7.2))
        fig, ax = plt.subplots()

        dataset_indices = []
        dataset_indices.append(self.output[pair_i]["data"][0])  # returns e.g. 1
        dataset_indices.append(self.output[pair_i]["data"][1])  # returns e.g. 2
        dataset_indices.append(self.output[pair_i]["data"][2])  # returns e.g. "1-2", the duplicates dataset is last, so that it is drawn on top of the others

        # affects the sequence in the legend and which one is drawn on top of each other
        for i, dataset_index in enumerate(dataset_indices):
            #c = [colors[f"{ds}"] for ds in df["dataset"].astype(int).values]
            dataset_labels = self.output[pair_i]["axis_label"]
            self.plot_one_dataset(ax, dataset_labels, i, dataset_indices)

        # x-axis date formatting
        plt.xlim(self.min_date - pd.Timedelta(days = 100), self.max_date + pd.Timedelta(days = 100))  # 100 days as a margin for plotting
        step_ = 0.05*(self.max_person_id - self.min_person_id)
        plt.ylim(self.min_person_id - step_, self.max_person_id + step_)
        
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
        plt.gcf().autofmt_xdate()
        plt.grid()

        label_0 = self.output[pair_i]["axis_label"][0]  # returns e.g. "OPENonOH"
        label_1 = self.output[pair_i]["axis_label"][1]
        plt.title(f"""{self.plot_config["title_prefix"]}\n{label_0}, {label_1}""")
        plt.xlabel("date")
        plt.ylabel("person index")
        plt.setp( plt.gca().get_xticklabels(),  rotation            = 30,
                                            horizontalalignment = 'right'
                                            )
        plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
        plt.tight_layout()

        print("saved figure: ", os.path.join(self.root_data_dir_name, self.output[pair_i]["img"][0], self.output[pair_i]["img"][1]))
        os.makedirs(os.path.join(self.root_data_dir_name, self.output[pair_i]["img"][0]), exist_ok=True)
        plt.savefig(os.path.join(self.root_data_dir_name, self.output[pair_i]["img"][0], self.output[pair_i]["img"][1]))


    def plot_one_dataset(self, ax, dataset_labels, i, dataset_indices):
        """
        plot one dataframe identified by the dataset variable.
        """
        # the data
        colors = self.plot_config["colors"]
        df = self.df

        selection = True
        color_, label_ = "", ""
        if i == 0:
            # pm_id as project member id
            selection =  ~pd.isna(df[f"pm_id_{dataset_indices[0]}"]) & pd.isna(df[f"pm_id_{dataset_indices[1]}"])
            color_ = colors[f"{dataset_indices[i]}"]
            label_ = dataset_labels[0]
        elif i == 1:
            selection =  pd.isna(df[f"pm_id_{dataset_indices[0]}"]) & ~pd.isna(df[f"pm_id_{dataset_indices[1]}"])
            color_ = colors[f"{dataset_indices[i]}"]
            label_ = dataset_labels[1]
        elif i == 2:  # duplicates
            selection =  ~pd.isna(df[f"pm_id_{dataset_indices[0]}"]) & ~pd.isna(df[f"pm_id_{dataset_indices[1]}"])
            color_ = colors["duplicates"]
            label_ = "duplicates"
        else:
            raise KeyError(f"i not in (0,1,2) should never happen: {i}")
        x = df.loc[selection, "date"].values
        y = df.loc[selection, "person_id"].values


        ax.scatter(x,y, marker='s', s=1, c=color_, label=f"{label_} ({len(x)} d)")
        
    def loop(self):
        """
        produces several plots, where one plot contains ds1, ds2 and their duplicates. They all share a common x-axis range.
        """
        for i, one_plot_config in enumerate(self.output):
            if "data" not in one_plot_config or "img" not in one_plot_config: continue
            print(i, one_plot_config)
            self.plot(i)


def main(config_filename : str = "IO.json", config_path : str = "."):
    pwp = pairwisePlot(config_filename, config_path)
    pwp.loop()


if __name__ == "__main__":
    fire.Fire(main)
