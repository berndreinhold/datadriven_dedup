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
    main()