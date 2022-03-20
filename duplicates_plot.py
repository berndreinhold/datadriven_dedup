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
        self.df[f"OpenAPS_duplicates"] = self.df[f"OpenAPS_duplicates"][["date", "user_id", "user_id_OPENonOH"]]
        self.df[f"OpenAPS_duplicates"]["user_id_OpenAPS"] = self.df[f"OpenAPS_duplicates"]["user_id"]
        self.df[f"OPENonOH_duplicates"] = self.df[f"OPENonOH_duplicates"][["date", "user_id", "user_id_OpenAPS"]]  
        self.df[f"OPENonOH_duplicates"]["user_id_OPENonOH"] = self.df[f"OPENonOH_duplicates"]["user_id"]

        # focus just on the date, user_id_* variables
        self.df[f"OpenAPS_duplicates"] = self.df[f"OpenAPS_duplicates"][["date", "user_id_OpenAPS", "user_id_OPENonOH"]]
        self.df[f"OPENonOH_duplicates"] = self.df[f"OPENonOH_duplicates"][["date", "user_id_OpenAPS", "user_id_OPENonOH"]]  
        
    def merge_all(self):
        """
        input: 
        the merged data frames OpenAPS_duplicates and OPENonOH_duplicates

        output: 
        df["merged_all"] with entries being uniquely identified by date, user_id_OpenAPS, user_id_OPENonOH

        note:
        an assumption here is that the sets of user_id_OpenAPS and user_id_OPENonOH are disjunct. Or if they are not disjunct, that they at least refer to the same person.
        If there are instances where user_id_OpenAPS == user_id_OPENonOH, they will be joined below, even though they might not be in the duplicates list.
        Probably a not very likely risk: See disjunct_user_ids().
        """

        # then merge these on user_id_OpenAPS, user_id_OPENonOH and date (outer join)
        self.df["merged_all"] = self.df[f"OpenAPS_duplicates"].merge(self.df["OPENonOH_duplicates"], left_on=["date", "user_id_OpenAPS", "user_id_OPENonOH"], 
            right_on=["date", "user_id_OpenAPS", "user_id_OPENonOH"], how="outer")
        print(self.df["merged_all"])



    def fine_tuning(self):
        """
        preparation of the dataframe for plotting: 
        1. introduce the "dataset" variable for sorting prior to plotting
        2. calculate an arbitrary auto-incremental index to plot instead of user_id_OpenAPS and user_id_OPENonOH
        """
        out = self.df["merged_all"]  # just an alias for easier readibility below
        out.loc[pd.isnull(out["user_id_OPENonOH"]), "dataset"] = 1  # OpenAPS
        out.loc[pd.isnull(out["user_id_OpenAPS"]), "dataset"] = 3  # OPENonOH
        out.loc[~(pd.isnull(out["user_id_OPENonOH"]) | pd.isnull(out["user_id_OpenAPS"])), "dataset"] = 2  # duplicates, partially overwrites 1 and 3
        
        out = out.sort_values(by=["dataset", "user_id_OpenAPS", "user_id_OPENonOH", "date"])
        
        # determine an auto-incremental index "id" for plotting instead of the user_id_OpenAPS, user_id_OPENonOH
        df_user_id = out[["user_id_OpenAPS", "user_id_OPENonOH"]].groupby(["user_id_OpenAPS", "user_id_OPENonOH"], as_index=False, dropna=False).agg("count")    
        df_user_id = df_user_id.sort_values(["user_id_OpenAPS", "user_id_OPENonOH"])
        df_user_id["id"] = range(len(df_user_id))


        # merge the df_user_id with the out dataset
        self.df["merged_all"] = out.merge(df_user_id, left_on=["user_id_OpenAPS", "user_id_OPENonOH"], right_on=["user_id_OpenAPS", "user_id_OPENonOH"], how="outer")
        #print(self.df["merged_all"])
        self.df["merged_all"] = self.df["merged_all"].groupby(["date", "id", "dataset"], as_index=False, dropna=False).agg("count")
        self.df["merged_all"] = self.df["merged_all"][["date", "id", "dataset"]]
    

    def plot(self):
        x = self.df["merged_all"]["date"].values
        y = self.df["merged_all"]["id"].values
        colors = {1: 'green', 2: 'yellow', 3: 'red'}
        c = [colors[ds] for ds in self.df["merged_all"]["dataset"].values]
        

        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=365))
        plt.scatter(x,y, marker='s', s=1, c=c)
        plt.gcf().autofmt_xdate()
        
        
        plt.grid()
        plt.title(self.json_output["title"])
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
        print("saved figure: ", os.path.join(self.json_output["img_path"][0], self.json_output["img_path"][1]))
        plt.savefig(os.path.join(self.json_output["img_path"][0], self.json_output["img_path"][1]))



def main():

    dp = duplicates_plot("IO.json", ".")
    #for ds in dp.dataset:
        #dp.df[ds].info()
    #dp.df["duplicates"].info()

    dp.merge_with_duplicates_dataset()
    dp.merge_all()
    dp.fine_tuning()
    dp.plot()

def test():
    dataset = ["OpenAPS", "OPENonOH"]
    outdir = "/home/reinhold/Daten/OPEN/"
    #outfilename = f"duplicates_{dataset[0]}_{dataset[1]}.csv"

    indir = [f"{outdir}/{d}_Data/csv_per_day/" for d in dataset]
    infilename = [f"entries_{d}.csv" for d in dataset]

    df = [pd.read_csv(os.path.join(indir[i], infilename[i]), header=0, parse_dates=[1], index_col=0) for i in range(2)]
    # add dataset variable to each dataset


def merge():
    

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



if __name__ == "__main__":
    main()
