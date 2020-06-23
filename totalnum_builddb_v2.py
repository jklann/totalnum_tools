import datetime as dt
import sqlite3
from os import listdir

import numpy as np
import pandas as pd

"""
  New version loads totalnum reports into a SQLite3 db from basedir (below) with the name format report_[siteid]_[foo].csv.
  Columns must be (in order) c_fullname, agg_date, agg_count. (Case insensitive on column names however.)
  Date format for agg_date (as enforced by the totalnum report script), should be YYYY-MM-DD, but the python parser can handle others.
  Bigfullnamefile must be a file with all possible paths (e.g., from the concept dimension) with columns: c_fullname, c_name.
  hlevel and "domain" are inferred.
  SQLite db uses a totalnum_int column in the totalnums table and puts this for reference in bigfullname.

 By Jeff Klann, PhD 05-2020
"""

""" Here's how I get the ontology data for the master list:
select distinct concept_path, name_char from concept_dimension

select distinct c_fullname, c_name, c_visualattributes, c_tooltip from act_covid
  (only the first two columns are needed)
"""

basedir = "/Users/jeffklann/HMS/Projects/ACT/totalnum_data/reports"
bigfullnamefile = '/Users/jeffklann/HMS/Projects/ACT/totalnum_data/ACT_covid_paths.csv'
conn = sqlite3.connect(basedir + '/totalnums.db')


def buildDb():
    # Build the main totalnums db
    files = [f for f in listdir(basedir) if ".csv" in f[-4:]]
    totals = []
    # Load the files
    for f in files:
        print(basedir + '/' + f)
        tot = totalnum_load(basedir + '/' + f)
        totals.append(tot)

    print(bigfullnamefile)
    bigfullname = pd.read_csv(bigfullnamefile,index_col='c_fullname')

    # Add c_hlevel, domain, and fullname_int columns
    bigfullname.insert(1, "c_hlevel", [x.count("\\") for x in bigfullname.index])
    bigfullname.insert(1, "domain", [x.split('\\')[2] if "PCORI_MOD" not in x else "MODIFIER" for x in bigfullname.index])
    bigfullname['fullname_int']=range(0,len(bigfullname))
    bigfullname.to_sql('bigfullname',conn,if_exists='replace')

    print("Converting path to int...")
    # Shrink the frame (remove c_name and fullname and hlevel and domain and add just the fullname_int)
    #outdf = delish.join(bigfullname,rsuffix='_bf',how='inner').reset_index()[['fullname_int','refresh_date','site','c']]
    outdf = pd.DataFrame()
    for t in totals:
        outdf=outdf.append(t)
    outdf=outdf.join(bigfullname,on='c_fullname',rsuffix='_bf',how='inner').reset_index()[['fullname_int','agg_date','agg_count','site']]
    print("Writing totalnum SQL...")
    # Temp step - use old style column names for compatibility
    outdf=outdf.rename(columns={'agg_date':'refresh_date','agg_count':'c'})
    outdf.to_sql("totalnums",conn,if_exists='replace', index=False)



    # Add indexes
    print("Indexing...")
    cur = conn.cursor()
    cur.execute("CREATE INDEX bfn_0 on bigfullname(c_hlevel)")
    cur.execute("CREATE INDEX bfn_int on bigfullname(fullname_int)")
    cur.execute("CREATE INDEX tot_int on totalnums(fullname_int)")

    print("Done!")

def totalnum_load(fname="",df=None):
    if not df:
        df = pd.read_csv(fname,index_col=0)
    # Remove null rows
    #df = df.loc[(df.ix[:,3:]!=0).any(axis=1)]
    # Lowercase totalnum columns
    df = df.reset_index().rename(columns=lambda x: x.lower())
    # Reorder columns
    df = df[['c_fullname','agg_date','agg_count']]
    # Convert totalnums to floats
    df = pd.concat([df.iloc[:,0:2],(df.iloc[:,2:].apply(pd.to_numeric,errors="coerce"))],axis=1)
    # And convert date string to datetime
    df = pd.concat([df.iloc[:, 0:1], pd.to_datetime(df['agg_date']),df.iloc[:,2]], axis=1)
    # Get site id out of report_siteid_blah.csv
    rfn = fname[::-1]
    fname_only = rfn[0:rfn.index('/')][::-1]
    fns = fname_only[fname_only.index('_')+1:]
    fns = fns[0:fns.index('_')]
    df['site']=fns

    return df

if __name__=='__main__':
    buildDb()