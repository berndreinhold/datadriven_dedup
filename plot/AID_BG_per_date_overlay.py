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




def common_pm_ids(root_data_dir_name, dataset_dir) -> pd.DataFrame:
    """
    calculate pm_ids that are common to the AID and the BG datasets and return it as a dataframe
    also calculate the person_counter variable
    
    Parameters:
    root_data_dir_name (str): root directory of the data

    Returns:
    pd.DataFrame: dataframe with the common pm_ids
    """
    df_pm_ids = []
    with open(os.path.join(root_data_dir_name, f"{dataset_dir}_Data", "AID_data_per_pm_id_date.csv")) as f:
        df = pd.read_csv(f, header=0, parse_dates=[1])
        df_pm_ids.append(df["pm_id"].unique())
    
    with open(os.path.join(root_data_dir_name, f"{dataset_dir}_Data", f"{dataset_dir.strip('/')}_per_day.csv")) as f:
        df = pd.read_csv(f, header=0, parse_dates=[1])
        df_pm_ids.append(df["pm_id"].unique())

    common_pm_ids_ = set(df_pm_ids[0]).intersection(set(df_pm_ids[1]))

    df2 = pd.DataFrame(data=list(common_pm_ids_), columns=["pm_id"])
    df2.sort_values(by="pm_id", inplace=True)
    df2["person_counter"] = np.arange(len(df2))
    return df2

# calculate set of project member ids, that have both AID and BG data
# use these as a selection of project member ids
# calculate the person_counter for these project member ids
# plot AID and BG data for these project member ids

def plot(root_data_dir_name, dataset_dir, suptitle, fig_name, df_pm_ids):
    df_AID, df_BG = pd.DataFrame(), pd.DataFrame()
    with open(os.path.join(root_data_dir_name, f"{dataset_dir}_Data", "AID_data_per_pm_id_date.csv")) as f:
        df = pd.read_csv(f, header=0, parse_dates=[1])
        df['date'] = df['date'].apply(lambda x: x.date())
        df = df.drop_duplicates(subset=["date", "pm_id"])  # this is necessary due to a mistake in the AID_data_per_pm_id_date.csv file

        df_AID = df.merge(df_pm_ids, on="pm_id", how="inner")  # select only the pm_ids that are common to both datasets

    with open(os.path.join(root_data_dir_name, f"{dataset_dir}_Data", f"{dataset_dir}_per_day.csv")) as f:
        df = pd.read_csv(f, header=0, parse_dates=[1])

        df_BG = df.merge(df_pm_ids, on="pm_id", how="inner")  # select only the pm_ids that are common to both datasets


    fig = plt.figure(figsize=(9.6, 7.2))
    gs = fig.add_gridspec(1, 2,  width_ratios=(7, 2), wspace=0.03)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharey=ax1)
    ax2.tick_params(axis="y", labelleft=False)
    

    fig.suptitle(f"{suptitle}")
    ax1.scatter(df_BG["date"].values, df_BG["person_counter"].values, marker='s', s=1, label="OPENonOH BG")
    ax1.scatter(df_AID["date"].values, df_AID["person_counter"].values, marker='s', s=1, label="OPENonOH AID", alpha=0.2)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=100))
    fig.autofmt_xdate()
    ax1.grid()
    ax1.legend()

    y = df_BG["person_counter"].values
    df_BG["person_counter"].hist(ax=ax2, bins=max(y)-min(y) + 1, orientation="horizontal")
    y = df_AID["person_counter"].values
    df_AID["person_counter"].hist(ax=ax2, bins=max(y)-min(y) + 1, orientation="horizontal", alpha=0.5)

    ax2.set_xlabel("number of measurements")

    ax1.set_xlabel("date")
    ax1.set_ylabel("person counter")
    plt.setp( ax1.get_xticklabels(),  rotation            = 30,
                                        horizontalalignment = 'right'
                                        )
    #plt.legend(loc="upper left", markerscale=4, framealpha=0.5)
    plt.tight_layout()
    #plt.show()
    plt.savefig(os.path.join(root_data_dir_name, "img", fig_name))





def main_AAPS_Uploader():
    root_data_dir_name = "/home/reinhold/Daten/dana_processing/"
    suptitle = """OPENonOH AID and BG data (AAPS_Uploader)"""
    fig_name = "AID_BG_data_overlay_OPENonOH_AAPS_Uploader_per_date.png"
    dataset_dir = "OPENonOH_AAPS_Uploader"
    df_pm_ids = common_pm_ids(root_data_dir_name, dataset_dir)
    plot(root_data_dir_name, dataset_dir, suptitle, fig_name, df_pm_ids)

def main_NS():
    root_data_dir_name = "/home/reinhold/Daten/dana_processing/"
    suptitle = """OPENonOH AID and BG data (NightScout)"""
    fig_name = "AID_BG_data_overlay_OPENonOH_NS_per_date.png"
    dataset_dir = "OPENonOH_NS"
    # preparation(os.path.join(root_data_dir_name, dataset_dir))
    df_pm_ids = common_pm_ids(root_data_dir_name, dataset_dir)
    plot(root_data_dir_name, dataset_dir, suptitle, fig_name, df_pm_ids)






if __name__ == "__main__":
    #main()
    main_AAPS_Uploader()
    main_NS()