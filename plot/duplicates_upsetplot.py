#/usr/bin/env python3

import matplotlib.pyplot as plt
import os
import json
import fire
from upsetplot import generate_counts
from upsetplot import plot

"""
call as: python3 duplicates_upsetplot.py [--config_filename=IO.json] [--config_path="."]

takes two dataset files and a duplicates file of these two datasets, which has been produced by other code (aggregation.py and preprocessing.py)
"""



def main(config_filename : str = "IO.json", config_path : str = "."):
    example = generate_counts()
    print(example)
    print(example[0])
    print(example[1])
    print(example[0][1])
    #print(type(example))
    #plot(example)
    #plt.show()

if __name__ == "__main__":
    fire.Fire(main)
