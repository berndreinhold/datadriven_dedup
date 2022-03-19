#/usr/bin/env python3
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os
import json

class duplicates_plot():

    def __init__(self, config_path : str, config_filename : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [user_id, date]
            - OPENonOH with key [user_id, date]
            - duplicates file containing the duplicates between the two data files with key [user_id_OpenAPS, user_id_OPENonOH, date]
        """
        pass

    def merge_with_duplicates_dataset(self):
        """
        both
        """

        # open 
        pass


def main():

    dataset = ["OpenAPS", "OPENonOH"]
    outdir = "/home/reinhold/Daten/OPEN/"
    #outfilename = f"duplicates_{dataset[0]}_{dataset[1]}.csv"

    indir = [f"{outdir}/{d}_Data/csv_per_day/" for d in dataset]
    infilename = [f"entries_{d}.csv" for d in dataset]

    df = [pd.read_csv(os.path.join(indir[i], infilename[i]), header=0, parse_dates=[1], index_col=0) for i in range(2)]
    df[0]["date"] = pd.to_datetime(df[0]["date"],format="%Y-%m-%d")
    # add dataset variable to each dataset



    # merge the OpenAPS and the duplicates dataset on [user_id, date]/[user_id_OpenAPS, date] (outer join) 
    # and the OPENonOH with the duplicates dataset on [user_id, date]/[user_id_OPENonOH, date] (outer join)
    # then merge these on user_id_OpenAPS, user_id_OPENonOH and user_id (outer join)
    out = df[0].merge(df[1], left_on="date", right_on="date", how="outer", suffixes=("_OpenAPS", "_OPENonOH"))
    # give priority to the dataset variable of the duplicate dataset over the other two    
    
    out = out[["date", "user_id_OpenAPS", "user_id_OPENonOH"]].sort_values(by=["user_id_OpenAPS", "user_id_OPENonOH", "date"])
    #out["dataset"] = 1 if out["user_id_OPENonOH"] is null
    out.loc[pd.isnull(out["user_id_OPENonOH"]), "dataset"] = 1  # OpenAPS
    out.loc[~(pd.isnull(out["user_id_OPENonOH"]) | pd.isnull(out["user_id_OpenAPS"])), "dataset"] = 2  # duplicates
    out.loc[pd.isnull(out["user_id_OpenAPS"]), "dataset"] = 3  # OPENonOH
    
    # determine an auto-incremental index for user_id_OpenAPS, user_id_OPENonOH
    df_user_id = out[["dataset", "user_id_OpenAPS", "user_id_OPENonOH"]].groupby(["dataset", "user_id_OpenAPS", "user_id_OPENonOH"], as_index=False).agg("count")    
    df_user_id = df_user_id.sort_values(["dataset", "user_id_OpenAPS", "user_id_OPENonOH"])
    df_user_id["id"] = range(len(df_user_id))
    print(df_user_id)
    
    out_OpenAPS = out.merge(df_user_id, left_on=["user_id_OpenAPS", "user_id_OPENonOH"], right_on=["user_id_OpenAPS", "user_id_OPENonOH"], how="inner")
    print(out_OpenAPS)
    out_OpenAPS = out_OpenAPS.groupby(["date", "id", "dataset"], as_index=False).agg("count")
    out_OpenAPS = out_OpenAPS[["date", "id", "dataset"]]
    
    print(out_OpenAPS)

	# df3 = df2[["user_id_OpenAPS", "user_id_OPENonOH", "date", "diff_sgv_mean", "diff_sgv_std", "diff_sgv_min", "diff_sgv_max", "diff_sgv_count", "second_id_OPENonOH", "filename_OPENonOH", "filename_OpenAPS"]].sort_values(by=["user_id_OpenAPS", "user_id_OPENonOH", "date"])
	#df3 = df3[[]]

	# group by statement has to return exactly one line, otherwise the selection criteria on diff_sgv_*-variables need to be tightened.


    # Todo: try group by without agg(), what happens then?

    x = out_OpenAPS["date"].values
    y = out_OpenAPS["index"].values

    #(fig, ax) = plt.subplots(1, 1)
  
    #ax.plot([x, y], lw=3)
    
    #plt.gca().xaxis.set_major_formatter(    matplotlib.ticker.FuncFormatter( lambda pos, _: time.strftime( "%d-%m-%Y %H:%M:%S", time.localtime( pos ) ) ) )
    #xfmt = mdates.DateFormatter('%y-%m-%d')
    #ax.xaxis.set_major_formatter(xfmt)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
    plt.scatter(x,y, marker='s', s=1)
    plt.gcf().autofmt_xdate()
    
    
    plt.grid()
    plt.title("OpenAPS")
    plt.xlabel("date")
    plt.ylabel("person (index)")
    #plt.yticks([1,2], labels=['target', 'hunter'])
    #plt.gca().set_ylim(0.0,2.5)
    plt.setp( plt.gca().get_xticklabels(),  rotation            = 30,
                                        horizontalalignment = 'right'
                                        )
    
    plt.tight_layout()
    #plt.show()

    #fig.show()
    plt.savefig("Duplicates.png")



if __name__ == "__main__":
    main()
