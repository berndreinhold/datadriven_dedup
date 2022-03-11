#/usr/bin/env python3
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os


def main():

    dataset = ["OpenAPS", "OPENonOH"]
    outdir = "/home/reinhold/Daten/OPEN/"
    #outfilename = f"duplicates_{dataset[0]}_{dataset[1]}.csv"

    indir = [f"{outdir}/{d}_Data/csv_per_day/" for d in dataset]
    infilename = [f"entries_{d}.csv" for d in dataset]

    df = [pd.read_csv(os.path.join(indir[i], infilename[i]), header=0, parse_dates=[1], index_col=0) for i in range(2)]
    df[0]["date"] = pd.to_datetime(df[0]["date"],format="%Y-%m-%d")
    out = df[0].merge(df[1], left_on="date", right_on="date", how="outer", suffixes=("_OpenAPS", "_OPENonOH"))



    df_user_id_OpenAPS = out[["date", "user_id_OpenAPS"]].groupby("user_id_OpenAPS").agg("count")
    columns = []
    for col in df_user_id_OpenAPS.columns:
        columns.append(f"{col[0]}_{col[1]}")
    df_user_id_OpenAPS.columns = columns
    df_user_id_OpenAPS.fillna(value=0, inplace=True)  #
    df_user_id_OpenAPS.reset_index(inplace=True)
    data = []
    for i,user_id in enumerate(sorted(df_user_id_OpenAPS["user_id_OpenAPS"].values)):
        data.append([i,user_id, "OpenAPS"])

    df = pd.DataFrame(data=data, columns = ["index", "user_id", "dataset"])
    print(df)
    out_OpenAPS = out.merge(df, left_on="user_id_OpenAPS", right_on="user_id", how="inner")
    out_OpenAPS = out_OpenAPS[["date", "index", "sgv_count_OpenAPS"]].groupby(["date", "index"]).agg("count")
    columns = []
    for col in out_OpenAPS.columns:
        columns.append(f"{col[0]}_{col[1]}")
    out_OpenAPS.columns = columns
    out_OpenAPS.fillna(value=0, inplace=True)  #
    out_OpenAPS.reset_index(inplace=True)
    out_OpenAPS = out_OpenAPS[["date", "index"]]

    #print(out_OpenAPS)

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
