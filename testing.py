import pandas as pd
import time
df = pd.read_csv('airport_freq.csv',low_memory=False)

grouped = df.groupby(df.description)
df_new = grouped.get_group('CTAF')
print(grouped.groups.keys())

import numpy as np

for d in np.array_split(df,4):
    print(d)