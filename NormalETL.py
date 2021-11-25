
import pandas as pd
import time
import splitFiles
import numpy as np

def extract(d):
    transform(d)

def transform(df):
    load(df)

def load(tdf):
    tdf.to_csv('airport_free_out.csv',mode='a',header=False,index=False)
    ##print(tdf.shape[0])
    return "load complete!!"

def main():
    file='airport_free.csv'
    dtype_dict = {'id': 'int8',
                  'airport_ref': 'int8',
                  'airport_ident': 'category',
                  'type': 'category',
                  'description': 'category',
                  'frequency_mhz': 'float16'
                  }
    df = pd.read_csv(file, dtype=dtype_dict, low_memory=False)
    st = time.time()
    chunk_size = int(df.shape[0] / 4)
    for start in range(0, df.shape[0], chunk_size):
        df_subset = df.iloc[start:start + chunk_size]
        extract(df_subset)
    en = time.time() - st
    print("etl 2 execution time {} sec".format(en))
    st = time.time()
    extract(df)
    en = time.time() - st
    print("etl 1 execution time {} sec".format(en))

    ##for d in lst:
    ##    st = time.time()
    ##    extract(d)
    ##    en = time.time()-st
    ##    print("etl execution time {} sec".format(en))


if __name__=="__main__":
    start1 = time.time()
    file = 'airport_free.csv'
    splitFiles.split_file(file)
    end1 = time.time() - start1
    print("File split completion time {} sec".format(end1))

    start = time.time()
    main()
    end = time.time() - start
    print("Total execution time {} sec".format(end))



