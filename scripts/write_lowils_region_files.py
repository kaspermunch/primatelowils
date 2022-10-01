
import sys, os
import pandas as pd
import numpy as np
from optimize import optimize_dataframe

_, input_file_name, output_file_name, cutoff_fraction = sys.argv

cutoff_fraction = float(cutoff_fraction)

sp1, sp2, sp3, sp4, _, chrom = os.path.splitext(os.path.basename(input_file_name))[0].split('_')

df = pd.read_hdf(input_file_name)
df['prop_ils'] = np.nan
df.loc[df.total > 0, 'prop_ils'] = df.ils / df.total
mean_ils = (df.ils / df.total).mean()
df['islow'] = df.prop_ils <= cutoff_fraction * mean_ils
df['run'] = (df.islow != df.islow.shift()).cumsum()

def start_end_coord(df):
    return pd.DataFrame(dict(
        start=[df.start.min()],
        end=[df.end.max()]))

df = (df
    .loc[df.islow]
    .groupby('run',  group_keys=False)
    .apply(start_end_coord)
    .reset_index(drop=True)
    .assign(sp1=sp1, sp2=sp2, sp3=sp3, sp4=sp4, chrom=chrom, mean_ils=mean_ils)
)
df = optimize_dataframe(df)

df.to_hdf(output_file_name, 'df', format='table')
