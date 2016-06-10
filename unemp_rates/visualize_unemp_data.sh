#!/bin/bash

# Use the ppsqlviz Python package with Anaconda Python to visualize the
# unemployment rate moving averages data.
# https://pypi.python.org/pypi/ppsqlviz/1.0.1

QUERY_FILE='./q1.sql'
QUERY_OUTPUT='unemp_data.out'

cat << _EOF > $QUERY_FILE
\set tblname window_demo.unemp

select month,
       unrate,
       mv5::numeric(6,2) as mov_avg_5,
       mv12::numeric(6,2) as mov_avg_12
from :tblname
where month between '2008-01-01' and '2012-12-31'
order by month
;
_EOF


psql -d gpuser -h gpdb-sandbox -U gpuser -f $QUERY_FILE -A -o $QUERY_OUTPUT

python -m 'ppsqlviz.plotter' tseries < $QUERY_OUTPUT &
