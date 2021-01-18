import datetime as dt
import sqlite3
from os import listdir

import numpy as np
import pandas as pd
import math

"""
ISSUES 12-15
NCATS_DEMOGRAPHICS and visit details - not even there
X Diagnoses ok
ACT Labs doesn't show up after "full list"
ACT Laboratory Tests no show at all
ACT Meds can't drill into
X Procedures ok
X COVID-19 broken
Visit details not there

"""

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
 and c_visualattributes not like '%H%' and c_synonym_cd!='Y'
  (only the first two columns are needed)
  
To do this for the whole ACT ontology, use my act_master_vw (separate script) and:
select distinct c_fullname, c_name, c_hlevel, c_visualattributes, c_tooltip from act_master_vw 
 where c_visualattributes not like '%H%' and c_synonym_cd!='Y'

"""

# Thanks https://stackoverflow.com/questions/2298339/standard-deviation-for-sqlite
class StdevFunc:
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 1

    def step(self, value):
        if value is None:
            return
        tM = self.M
        self.M += (value - tM) / self.k
        self.S += (value - tM) * (value - self.M)
        self.k += 1

    def finalize(self):
        if self.k < 3:
            return None
        return math.sqrt(self.S / (self.k-2))

basedir = "/Users/jeffklann/HMS/Projects/ACT/totalnum_data/reports"
bigfullnamefile = '/Users/jeffklann/HMS/Projects/ACT/totalnum_data/ACT_paths_full.csv' # ACT_covid_paths_v3.csv
conn = sqlite3.connect(basedir + '/totalnums.db')
conn.create_aggregate("stdev", 1, StdevFunc)

""" SQL code that creates views and additional tables on the totalnum db for analytics
"""
def postProcess():
   sql = r"""
   -- Create a pre-joined view for faster coding
    drop view if exists totalnums_recent_joined;

    create view totalnums_recent_joined as
    select c_hlevel,domain,c_visualattributes,f.fullname_int,c_fullname,c_name,agg_date,agg_count,site from 
    bigfullname f left join totalnums_recent t on f.fullname_int=t.fullname_int;

   -- Create a view with old column names
   drop view if exists totalnums_oldcols;
   
   create view totalnums_oldcols as 
     SELECT fullname_int, agg_date AS refresh_date, agg_count AS c, site 
	FROM totalnums;
   
   drop view if exists totalnums_recent;

   -- Set up view for most recent totalnums
    create view totalnums_recent as 
    select t.* from totalnums t inner join 
    (select fullname_int, site, max(agg_date) agg_date from totalnums group by fullname_int, site) x 
     on x.fullname_int=t.fullname_int and x.site=t.site and x.agg_date=t.agg_date;
     
    -- Get denominator: any pt in COVID ontology (commented out is any lab test which works better if the site has lab tests)
    drop view if exists anal_denom;

    create view anal_denom as
    select site, agg_count denominator from totalnums_recent where fullname_int in
    (select fullname_int from bigfullname where c_fullname='\ACT\UMLS_C0031437\SNOMED_3947185011\');--UMLS_C0022885\')

    -- View total / denominator = pct
    drop view if exists totalnums_recent_pct;
    
    create view totalnums_recent_pct as
    select fullname_int, agg_date, cast(cast(agg_count as float) / denominator * 100 as int) pct, tot.site from totalnums_recent tot inner join anal_denom d on tot.site=d.site; 
    
    -- Site outliers: compute avg and stdev.
    -- I materialize this (rather than a view) because SQLite doesn't have a stdev function.
    drop table if exists outliers_sites;
        
    create table outliers_sites as
    select agg_count-stdev-average,* from totalnums_recent r inner join
    (select * from
    (select fullname_int,avg(agg_count) average, stdev(agg_count) stdev, count(*) num_sites from totalnums_recent r  where agg_count>-1 group by fullname_int) 
     where num_sites>1) stat on stat.fullname_int=r.fullname_int;
     
    -- Site outliers: compute avg and stdev.
    -- I materialize this (rather than a view) because SQLite doesn't have a stdev function.
    drop table if exists outliers_sites_pct;
        
    create table outliers_sites_pct as
    select pct-stdev-average,* from totalnums_recent_pct r inner join
    (select * from
    (select fullname_int,avg(pct) average, stdev(pct) stdev, count(*) num_sites from totalnums_recent_pct r  where pct>=0 group by fullname_int) 
     where num_sites>1) stat on stat.fullname_int=r.fullname_int;

    -- Add some fullnames for summary measures and reporting
    drop table if exists  toplevel_fullnames;

    create table toplevel_fullnames as
    select fullname_int from bigfullname where c_fullname like '\ACT\Diagnosis\ICD10\%' and c_hlevel=2 and c_visualattributes not like 'L%'
    union all
    select fullname_int from bigfullname where c_fullname like '\ACT\Diagnosis\ICD9\V2_2018AA\A18090800\%' and c_hlevel=2 and c_visualattributes not like 'L%'
    union all
    select fullname_int from bigfullname where c_fullname like '\ACT\Procedures\CPT4\V2_2018AA\A23576389\%' and c_hlevel=2 and c_visualattributes not like 'L%'
    union all
    select fullname_int from bigfullname where c_fullname like '\ACT\Procedures\HCPCS\V2_2018AA\A13475665\%' and c_hlevel=2 and c_visualattributes not like 'L%'
    union all
    select fullname_int from bigfullname where c_fullname like '\ACT\Procedures\ICD10\V2_2018AA\A16077350\%' and c_hlevel=2 and c_visualattributes not like 'L%'
    union all
    select fullname_int from bigfullname where c_fullname like '\ACT\Lab\LOINC\V2_2018AA\%' and c_hlevel=7 and c_visualattributes not like 'L%'
    union all
    select fullname_int from bigfullname where c_fullname like '\ACT\Medications\MedicationsByVaClass\V2_09302018\%' and c_hlevel=5 and c_visualattributes not like 'L%';
    
    create index toplevel_fullnames_f on toplevel_fullnames(fullname_int);

   """
   cur = conn.cursor()
   cur.executescript(sql)
   cur.close()

def buildDb():
    # Build the main totalnums db
    files = [f for f in listdir(basedir) if ".csv" in f[-4:]]
    totals = []
    # Load the files
    for f in files:
        print(basedir + '/' + f)
        tot = totalnum_load(basedir + '/' + f)
        totals.append(tot)

    # 11-20 - support both utf-8 and cp1252
    print(bigfullnamefile)
    bigfullname = None
    try:
        bigfullname = pd.read_csv(bigfullnamefile,index_col='c_fullname',delimiter=',',dtype='str')
    except UnicodeDecodeError:
        bigfullname = pd.read_csv(bigfullnamefile,index_col='c_fullname',delimiter=',',dtype='str',encoding='cp1252')

    # Add c_hlevel, domain, and fullname_int columns
    if "c_hlevel" not in bigfullname.columns: bigfullname.insert(1, "c_hlevel", [x.count("\\") for x in bigfullname.index])
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
    #outdf=outdf.rename(columns={'agg_date':'refresh_date','agg_count':'c'})
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
        # Support both utf-8 and cp1252
        try:
            df = pd.read_csv(fname,index_col=0)
        except UnicodeDecodeError:
            df = pd.read_csv(fname, index_col=0,encoding='cp1252')
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
    print("SQLite Version is:", sqlite3.sqlite_version)
    buildDb()
    postProcess()
    None