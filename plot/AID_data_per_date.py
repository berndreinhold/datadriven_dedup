#/usr/bin/env python3
from operator import index
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os
import glob
import json
import fire
import numpy as np
from numpy import nan


def preparation():
    root_data_dir_name = "/home/reinhold/Daten/dana_processing/OPENonOH_AAPS_Uploader_Data/"
    
    df_list = []
    for i, f in enumerate(glob.glob(os.path.join(root_data_dir_name, "per_measurement_csv_AID/", "**", "*APSData.csv"), recursive=True)):
        #if i%500==0: print(i, f)
        df_list.append(pd.read_csv(f, header=0, parse_dates=[1]))
        df_list[-1]["date"] = pd.to_datetime(df_list[-1]["date_"],format="%Y-%m-%d")
        df_list[-1]["pm_id"] = df_list[-1]["pm_id"].astype(int)

    df = pd.concat(df_list)
    df2 = df[["date","pm_id"]].drop_duplicates()
    
    df2.info()
    df2.to_csv(os.path.join(root_data_dir_name,"AID_data_per_pm_id_date.csv"))
    return df2

def plot():
    root_data_dir_name = "/home/reinhold/Daten/dana_processing/OPENonOH_AAPS_Uploader_Data/"
    df3 = pd.DataFrame()
    with open(os.path.join(root_data_dir_name, "AID_data_per_pm_id_date.csv")) as f:
        df = pd.read_csv(f, header=0, parse_dates=[1])
        df2 = pd.DataFrame(data=df["pm_id"].unique(), columns=["pm_id"])
        df2.sort_values(by="pm_id", inplace=True)
        df2["person_id"] = np.arange(len(df2))

        df3 = df.merge(df2, on="pm_id")

    fig = plt.figure(figsize=(9.6, 7.2))
    gs = fig.add_gridspec(1, 2,  width_ratios=(7, 2), wspace=0.03)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharey=ax1)
    ax2.tick_params(axis="y", labelleft=False)
    
    label_0 = "OPENonOH AID data"  # returns e.g. "OPENonOH"
    label_1 = "project member ID"
    fig.suptitle(f"""OPENonOH AID data (AAPS_Uploader)\n{label_0}, {label_1}""")

    x = df3["date"].values
    y = df3["person_id"].values
    print (x, y)
    
    ax1.scatter(x,y, marker='s', s=1)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=100))
    fig.autofmt_xdate()
    ax1.grid()

    df3["person_id"].hist(ax=ax2, bins=max(y)-min(y) + 1, orientation="horizontal")
    ax2.set_xlabel("number of measurements")

    ax1.set_xlabel("date")
    ax1.set_ylabel("person ID")
    plt.setp( ax1.get_xticklabels(),  rotation            = 30,
                                        horizontalalignment = 'right'
                                        )
    #plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
    plt.tight_layout()
    plt.show()
    plt.savefig(os.path.join(root_data_dir_name, "img", "AID_data_OPENonOH_AAPS_Uploader_per_date.png"))



def main_AAPS_Uploader():
    #preparation()
    plot()


def test():

    df = pd.read_csv(os.path.join(root_data_dir_name, input[0], input[1]), header=0, parse_dates=[1])
    df["date"] = pd.to_datetime(df["date"],format="%Y-%m-%d")
    df["pm_id_1"] = df[" pm_id_1"].astype(int)
    df2 = pd.read_csv(os.path.join(root_data_dir_name, input2[0], input2[1]), header=0)
    df2["person_id"] = range(len(df2))
    df2["pm_id_1"] = df2[" pm_id_1"].astype(int)
    print(df.columns, df2.columns)
    df3 = df.merge(df2, on="pm_id_1")
    
    print(df3)

    plt.figure(figsize=(9.6, 7.2))
    fig, ax = plt.subplots()

    
    label_0 = "OPENonOH AID data"  # returns e.g. "OPENonOH"
    label_1 = "project member ID"
    
    x = df3["date"].values
    y = df3["person_id"].values
    print (x, y)
    
    ax.scatter(x,y, marker='s', s=1)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
    plt.gcf().autofmt_xdate()
    plt.grid()

    plt.title(f"""OPENonOH AID data (NS)\n{label_0}, {label_1}""")
    plt.xlabel("date")
    plt.ylabel("person ID")
    plt.setp( plt.gca().get_xticklabels(),  rotation            = 30,
                                        horizontalalignment = 'right'
                                        )
    #plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(root_data_dir_name, "img", "AID_data_OPENonOH_NS_per_date.png"))



def main():
    root_data_dir_name = "/home/reinhold/Daten/dana_processing/"
    input = ["OPENonOH_NS_Data", "AID_data_per_pm_id_date.csv"]
    input2 = ["OPENonOH_NS_Data", "AID_data_per_pm_id.csv"]

    df = pd.read_csv(os.path.join(root_data_dir_name, input[0], input[1]), header=0, parse_dates=[1])
    df["date"] = pd.to_datetime(df["date"],format="%Y-%m-%d")
    df["pm_id_1"] = df[" pm_id_1"].astype(int)
    df2 = pd.read_csv(os.path.join(root_data_dir_name, input2[0], input2[1]), header=0)
    df2["person_id"] = range(len(df2))
    df2["pm_id_1"] = df2[" pm_id_1"].astype(int)
    print(df.columns, df2.columns)
    df3 = df.merge(df2, on="pm_id_1")
    
    print(df3)

    plt.figure(figsize=(9.6, 7.2))
    fig, ax = plt.subplots()

    
    label_0 = "OPENonOH AID data"  # returns e.g. "OPENonOH"
    label_1 = "project member ID"
    
    x = df3["date"].values
    y = df3["person_id"].values
    print (x, y)
    
    ax.scatter(x,y, marker='s', s=1)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
    plt.gcf().autofmt_xdate()
    plt.grid()

    plt.title(f"""OPENonOH AID data (NS)\n{label_0}, {label_1}""")
    plt.xlabel("date")
    plt.ylabel("person ID")
    plt.setp( plt.gca().get_xticklabels(),  rotation            = 30,
                                        horizontalalignment = 'right'
                                        )
    #plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(root_data_dir_name, "img", "AID_data_OPENonOH_NS_per_date.png"))


if __name__ == "__main__":
    #main()
    main_AAPS_Uploader()