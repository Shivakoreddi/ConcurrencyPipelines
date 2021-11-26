from concurrent.futures import ThreadPoolExecutor
import threading
from threading import *
import pandas as pd
import time

lock = Lock()
semTrn = Semaphore(4)
semLd = Semaphore(4)

def extract(file):
    dtype_dict = {'id': 'category',
                  'airport_ref': 'category',
                  'airport_ident': 'category',
                  'type': 'category',
                  'description': 'category',
                  'frequency_mhz': 'float16'
                  }

    df = pd.read_csv(file, dtype=dtype_dict, low_memory=False)
    return df


def transform(df):
    ##semaphore lock
    semTrn.acquire()
    print("thread {} acquired tranform lock ".format(threading.currentThread().ident))
    ##basic transformation operation

    df['ref_code'] = df['airport_ident'].astype(str)+str('***')
    semTrn.release()
    print("thread {} released tranform lock ".format(threading.currentThread().ident))
    print("thread {} acquired load lock ".format(threading.currentThread().ident))
    semLd.acquire()
    load(df)


def load(tdf):

    tdf.to_csv('airport_freq_output.csv', mode='a', header=False, index=False)
    semLd.release()
    print("thread {} released load lock  ".format(threading.currentThread().ident))
    print("thread {} load completion ".format(threading.currentThread().ident))

def main():
    pd.set_option('mode.chained_assignment', None)
    file = 'airport_freq.csv'
    df = extract(file)
    chunk_size = int(df.shape[0] / 4)
    ##t = [0] * 4
    executor = ThreadPoolExecutor(max_workers=4)
    lst = list()
    for start in range(0, df.shape[0], chunk_size):
        df_subset = df.iloc[start:start + chunk_size]
        ##df_subset.is_copy=None
        lst.append(executor.submit(transform, df_subset))
    for future in lst:
        future.result()
    executor.shutdown()

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time() - start
    print("Execution time {} sec".format(end))
