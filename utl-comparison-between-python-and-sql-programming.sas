%let pgm=utl-comparison-between-python-and-sql-programming;

Comparison between python and sql programming

 SOLUTIONS

      1. SAS SQL
      2. WPS SQL
      3. WPS PROC R
      4. PYTHON SQL
      5. R SQL
      6. PYTHON NON SQL
         I could not get the provided sql to work


github
https://tinyurl.com/2746y69h
https://github.com/rogerjdeangelis/utl-comparison-between-python-and-sql-programming

StackOverflow
https://tinyurl.com/mvu3mspr
https://stackoverflow.com/questions/74625195/issues-in-converting-sas-macro-to-pandas

REPRODUCE THIS SAS ALGORITHM IN SQL

data example;
   input group_v1;
   retain group 0;
         if _n_=1 and group_v1 = -1  then group = group_v1 ;
    else if _n_=1 and group_v1 ne -1 then group = 0        ;
    else                                  group = group+1  ;
cards4;
-1
0
1
11
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  REPRODUCE THIS OUTPUT IN SQL                                                                                          */
/*                                                                                                                        */
/*  Up to 40 obs from last table SD1.HAVE total obs=4 01MAY2023:08:24:33                                                  */
/*                                                                                                                        */
/*                     COMPUTE THIS                                                                                       */
/*  Obs    GROUP_V1    GROUP                                                                                              */
/*                                                                                                                        */
/*   1         -1        -1                                                                                               */
/*   2          0         0                                                                                               */
/*   3          1         1                                                                                               */
/*   4         11         2                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*---- remove group variable ----*/

libname sd1 "d:/sd1";

data sd1.have;
  set example(drop=group);
run;quit;

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  Up to 40 obs from last table SD1.HAVE total obs=4 01MAY2023:09:11:34                                                  */
/*                                                                                                                        */
/*  Obs    GROUP_V1                                                                                                       */
/*                                                                                                                        */
/*   1         -1                                                                                                         */
/*   2          0                                                                                                         */
/*   3          1                                                                                                         */
/*   4         11                                                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                         _
 ___  __ _ ___   ___  __ _| |
/ __|/ _` / __| / __|/ _` | |
\__ \ (_| \__ \ \__ \ (_| | |
|___/\__,_|___/ |___/\__, |_|
                        |_|
*/

proc sql;
  create
    table want_sasSql as
 select
   group_v1
  ,case (rn)
     when (1) then -1
     else          rn-2
   end as group
  from
   (select group_v1, monotonic() as rn from sd1.have )
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from WANT_SASSQL total obs=4 01MAY2023:08:30:12                                                           */
/*                                                                                                                        */
/* Obs    GROUP_V1   GROUP                                                                                                */
/*                                                                                                                        */
/*  1         -1       -1                                                                                                 */
/*  2          0        0                                                                                                 */
/*  3          1        1                                                                                                 */
/*  4         11        2                                                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

 /*                              _
__      ___ __  ___   ___  __ _| |
\ \ /\ / / `_ \/ __| / __|/ _` | |
 \ V  V /| |_) \__ \ \__ \ (_| | |
  \_/\_/ | .__/|___/ |___/\__, |_|
         |_|                 |_|
*/

%utl_submit_wps64('

options validvarname=any;

libname sd1 "d:/sd1";

proc sql;
  create
    table want_wpsSql as
  select
     group_v1
    ,case (rn)
      when (1) then -1
      else          rn-2
    end as group
  from
    (select group_v1, monotonic() as rn from sd1.have )
;quit;

proc print;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    GROUP_V1    group                                                                                               */
/*                                                                                                                        */
/*  1        -1         -1                                                                                                */
/*  2         0          0                                                                                                */
/*  3         1          1                                                                                                */
/*  4        11          2                                                                                                */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*
__      ___ __  ___   _ __  _ __ ___   ___   _ __
\ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__|
 \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |
  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|
         |_|         |_|
*/

%utl_submit_wps64("
 libname sd1 'd:/sd1';
 proc r;
   export data=sd1.have r=have;
 submit;
 library(sqldf);
 want_r <- sqldf('
  select
     group_v1
    ,case (rn)
       when (1) then -1
       else sum(1) OVER ( ORDER BY rn ASC) - 2
    end as group_r
  from
    (select group_v1, row_number() OVER (ORDER BY group_v1 ASC) as rn from have)
 ');
 want_r;
 endsubmit;
  import data=want_r r=want_r;
 proc print;
 run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* bs    GROUP_V1    GROUP_R                                                                                              */
/*                                                                                                                        */
/* 1        -1          -1                                                                                                */
/* 2         1           0                                                                                                */
/* 3         1           1                                                                                                */
/*                                                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*           _   _                             _
 _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
| .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
|_|    |___/                                |_|
*/


%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
import sqlite3;
print('sqlite3 sqlite_version: ', sqlite3.sqlite_version)
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
want   = pdsql("""
 select
   group_v1
  ,case (rn)
    when (1) then -1
    else sum(1) OVER ( ORDER BY rn ASC) - 2
   end as group_py
  from
   ( select group_v1, row_number() OVER (ORDER BY group_v1 ASC) as rn from have )
""")
print(want);
ds = xport.Dataset(want, name='want')
with open('d:/xpt/want.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;


libname pyxpt xport "d:/xpt/want.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.want;
run;quit;

data want;
   set pyxpt.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from WANT total obs=4 01MAY2023:11:30:15                                                                  */
/*                                                                                                                        */
/* Obs    GROUP_V1    GROUP_PY                                                                                            */
/*                                                                                                                        */
/*  1         -1         -1                                                                                               */
/*  2          0          0                                                                                               */
/*  3          1          1                                                                                               */
/*  4         11          2                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                _
 _ __   ___  __ _| |
| `__| / __|/ _` | |
| |    \__ \ (_| | |
|_|    |___/\__, |_|
               |_|
*/

%utl_submit_r64('
 library(sqldf);
 library(haven);
 library(SASxport);
 have<-read_sas("d:/sd1/have.sas7bdat");
 want_r <- sqldf("
  select
     group_v1
    ,case (rn)
       when (1) then -1
       else sum(1) OVER ( ORDER BY rn ASC) - 2
    end as group_r
  from
    (select group_v1, row_number() OVER (ORDER BY group_v1 ASC) as rn from have)
 ");
  write.xport(want_r,file="d:/xpt/want_r.xpt");
');

libname xpt xport "d:/xpt/want.xpt";

proc print data=xpt.want;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from WANT total obs=4 01MAY2023:11:30:15                                                                  */
/*                                                                                                                        */
/* Obs    GROUP_V1    GROUP_PY                                                                                            */
/*                                                                                                                        */
/*  1         -1         -1                                                                                               */
/*  2          0          0                                                                                               */
/*  3          1          1                                                                                               */
/*  4         11          2                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*           _   _                         _   _
 _ __   __ _| |_(_)_   _____   _ __  _   _| |_| |__   ___  _ __
| `_ \ / _` | __| \ \ / / _ \ | `_ \| | | | __| `_ \ / _ \| `_ \
| | | | (_| | |_| |\ V /  __/ | |_) | |_| | |_| | | | (_) | | | |
|_| |_|\__,_|\__|_| \_/ \___| | .__/ \__, |\__|_| |_|\___/|_| |_|
                              |_|    |___/
*/

%utl_pybegin;
parmcards4;
import pandas as pd;
tmp1 = pd.DataFrame({"group_v1": [-1, 0, 1, 11]})
print(tmp1)
def build_tmp2(tmp1):
  # Contains the new rows for tmp2
  _tmp2 = []
  # Loop over the rows of tmp1 - like a data step does
  for i, row in tmp1.iterrows():
    # equivalent to the data statement - copy the current row to memory
    tmp2 = row.copy()
    # _N_ is equivalent to i, except i starts at zero in Pandas/Python
    if i == 0:
      # Create a new variable called pdv to contain values across loops
      # This is equivalent to the Program Data Vector in SAS
      pdv = {}
      if row['group_v1'] == -1:
        pdv['group'] = row['group_v1']
      else:
        pdv['group'] = 0
    # Equivalent to both retain group and also group=group+1
    pdv['group']+=1
    # Copy the accumulating group variable to the target row
    tmp2['group'] = pdv['group']
    # Append the updated row to the list
    _tmp2.append(tmp2.copy())
  # After the loop has finished build the new DataFrame from the list
  return pd.DataFrame(_tmp2)

want=build_tmp2(tmp1)
print(want)
;;;;
%utl_pyend;

/**************************************************************************************************************************/
/*                       |                                                                                                */
/*    group_v1  group    |  Should be                                                                                     */
/* 0        -1      0    |     -1                                                                                         */
/* 1         0      1    |      0                                                                                         */
/* 2         1      2    |      1                                                                                         */
/* 3        11      3    |      2                                                                                         */
/*                       |                                                                                                */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
