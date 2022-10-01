
import os, sys, re, glob
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from chromwindow import window
from optimize import optimize_dataframe

_, segment_file_name, window_file_name = sys.argv

@window(size=100000)
def ils_proportion(df):
    _df = df.loc[df.state.isin(['V2', 'V3'])]
    return {'ils': (_df.end - _df.start).sum(), 'total':  (df.end - df.start).sum()}

sp1, sp2, sp3, sp4, _, chrom = os.path.splitext(os.path.basename(segment_file_name))[0].split('_')
df = pd.read_hdf(segment_file_name)
ils_proportion(df).assign(chrom=chrom).to_hdf(window_file_name, 'df', format='table')

