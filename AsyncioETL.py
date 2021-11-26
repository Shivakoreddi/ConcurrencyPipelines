import asyncio
import time

import pandas as pd
from memory_profiler import profile
##coroutine - wrapped version of function to run async
##async function in python is typically called coroutine
##coroutines declared with async/await syntax
##coroutines are special functions that return coroutine objects when called


##event loop -
##the event loop is a very efficient task manager
##coordinate tasks
##run asynchronous tasks and callbacks

def extract(file):
    dtype_dict = {'id': 'category',
                  'airport_ref': 'category',
                  'airport_ident': 'category',
                  'type': 'category',
                  'description': 'category',
                  'frequency_mhz': 'float16'
                  }
    df = pd.read_csv(file, dtype=dtype_dict,low_memory=False)
    return df

async def transform(df):

    df['ref_code'] = df['airport_ident'].astype(str)+str('***')
    await load(df)

async def load(tdf):

    tdf.to_csv('airport_freq_out.csv', mode='a', header=False, index=False)
    await asyncio.sleep(0)



async def main():
    pd.set_option('mode.chained_assignment', None)
    file = 'airport_freq.csv'
    df = extract(file)
    chunk_size = int(df.shape[0] / 4)
    for start in range(0, df.shape[0], chunk_size):
        df_subset = df.iloc[start:start + chunk_size]
        x = asyncio.create_task(transform(df_subset))
        await x



start = time.time()
asyncio.run(main())
end=time.time()-start
print("execution time {} sec".format(end))