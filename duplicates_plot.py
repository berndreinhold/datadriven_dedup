#/usr/bin/env python3
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import matplotlib.dates as mdates
import os
import json

class duplicates_plot():

    def __init__(self, config_filename : str, config_path : str):
        """
        read a config file and populate file names and paths of three csv files:
            - OpenAPS with key [user_id, date]
            - OPENonOH with key [user_id, date]
            - duplicates file containing the duplicates between the two data files with key [user_id_OpenAPS, user_id_OPENonOH, date]
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.json_input = IO_json["duplicates_plot"]["input"]
        self.json_output = IO_json["duplicates_plot"]["output"]

        self.dataset = ["OpenAPS", "OPENonOH", "duplicates"]
        self.df = {}
        for ds in self.dataset:
            self.df[ds] = pd.read_csv(os.path.join(*self.json_input[ds]), header=0, parse_dates=[1], index_col=0)
            self.df[ds]["date"] = pd.to_datetime(self.df[ds]["date"],format="%Y-%m-%d")
            # fix the data types that were loaded as the unspecific "object"
            for col in self.df[ds].columns:
                if "filename" in col:
                    self.df[ds][col] = self.df[ds][col].astype('string')

        # fix the data types that were loaded as the unspecific "object"
        self.df["duplicates"]['user_id_OpenAPS'] = self.df["duplicates"]['user_id_OpenAPS'].astype(int)


    def merge_with_duplicates_dataset(self):
        """
        input: the three data frames "OpenAPS", "OPENonOH", "duplicates"

        output and algo: 
        df["OpenAPS_duplicates"]: merge the OpenAPS and the duplicates dataset on [user_id, date]/[user_id_OpenAPS, date] (outer join) 
        df["OPENonOH_duplicates"]: merge the OPENonOH with the duplicates dataset on [user_id, date]/[user_id_OPENonOH, date] (outer join)
        
        add merged data frames OpenAPS_duplicates and OPENonOH_duplicates to self.df-dictionary of data frames
        """
        for ds in self.dataset[:2]:
            self.df[f"{ds}_duplicates"] = self.df[ds].merge(self.df["duplicates"], left_on=["date", "user_id"], right_on=["date", f"user_id_{ds}"], 
                how="outer", suffixes=(f"_{ds}", None))

        # prepare the user_id variables for the merge of OpenAPS_duplicates and OPENonOH_duplicates
        self.df[f"OpenAPS_duplicates"] = self.df[f"OpenAPS_duplicates"]["date", "user_id", "user_id_OPENonOH"]
        self.df[f"OpenAPS_duplicates"]["user_id_OpenAPS"] = self.df[f"OpenAPS_duplicates"]["user_id"]
        self.df[f"OPENonOH_duplicates"] = self.df[f"OPENonOH_duplicates"]["date", "user_id", "user_id_OpenAPS"]  
        self.df[f"OPENonOH_duplicates"]["user_id_OPENonOH"] = self.df[f"OPENonOH_duplicates"]["user_id"]

        # focus just on the date, user_id_* variables
        self.df[f"OpenAPS_duplicates"] = self.df[f"OpenAPS_duplicates"]["date", "user_id_OpenAPS", "user_id_OPENonOH"]
        self.df[f"OPENonOH_duplicates"] = self.df[f"OPENonOH_duplicates"]["date", "user_id_OpenAPS", "user_id_OPENonOH"]  
        
    def merge(self):
        
        # then merge these on user_id_OpenAPS, user_id_OPENonOH and date (outer join)
        self.df["merged_all"] = self.df[f"OpenAPS_duplicates"].merge(self.df["OPENonOH_duplicates"], left_on=["date", "user_id_OpenAPS", "user_id_OPENonOH"], 
            right_on=["date", "user_id_OpenAPS", "user_id_OPENonOH"], how="outer")


    def fine_tuning(self):
        #out = out[["date", "user_id_OpenAPS", "user_id_OPENonOH"]].sort_values(by=["user_id_OpenAPS", "user_id_OPENonOH", "date"])
        #out["dataset"] = 1 if out["user_id_OPENonOH"] is null
        #out.loc[pd.isnull(out["user_id_OPENonOH"]), "dataset"] = 1  # OpenAPS
        #out.loc[~(pd.isnull(out["user_id_OPENonOH"]) | pd.isnull(out["user_id_OpenAPS"])), "dataset"] = 2  # duplicates
        #out.loc[pd.isnull(out["user_id_OpenAPS"]), "dataset"] = 3  # OPENonOH
        pass

    def plot(self):
        pass        




def main():

    dp = duplicates_plot("IO.json", ".")
    #for ds in dp.dataset:
        #dp.df[ds].info()
    #dp.df["duplicates"].info()

    dp.merge_with_duplicates_dataset()


def test():
    dataset = ["OpenAPS", "OPENonOH"]
    outdir = "/home/reinhold/Daten/OPEN/"
    #outfilename = f"duplicates_{dataset[0]}_{dataset[1]}.csv"

    indir = [f"{outdir}/{d}_Data/csv_per_day/" for d in dataset]
    infilename = [f"entries_{d}.csv" for d in dataset]

    df = [pd.read_csv(os.path.join(indir[i], infilename[i]), header=0, parse_dates=[1], index_col=0) for i in range(2)]
    # add dataset variable to each dataset


def merge():
    
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
