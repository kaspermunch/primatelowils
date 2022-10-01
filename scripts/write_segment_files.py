
import os, sys, re, glob
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from chromwindow import window
from optimize import optimize_dataframe

_, post_prob_file_name, segment_file_name = sys.argv

def start_end_coord(df):
    return DataFrame(dict(start=[df.Homo_sapiens.min()], end=[df.Homo_sapiens.max()+1], state=[df.map_state.iloc[0]]))

sp1, sp2, sp3, sp4, _, chrom = os.path.splitext(os.path.basename(post_prob_file_name))[0].split('_')
df = pd.read_hdf(post_prob_file_name)
df.set_index('Homo_sapiens', inplace=True)
df['map_state'] = df.idxmax(axis=1)
df['sgm_idx'] = (df.map_state != df.map_state.shift()).cumsum()
df.reset_index(inplace=True)
df = (df
            .loc[df.Homo_sapiens != -1]
            .reset_index()
            .groupby('sgm_idx', group_keys=False)
            .apply(start_end_coord)
            .assign(sp1=sp1, sp2=sp2, sp3=sp3, sp4=sp4, chrom=chrom)
            )
#gc.collect()            
df = optimize_dataframe(df)
#gc.collect()            
df.to_hdf(segment_file_name, 'df', format='table')
