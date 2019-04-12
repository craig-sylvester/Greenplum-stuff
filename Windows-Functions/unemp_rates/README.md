## Calculating Moving Averages using Window functions

A common task in economic analysis is the calculation of moving averages over a given time period. The example here uses unemployment rate data for every month starting with the year 1948.

The visualize_unemp_data.sh script was created to plot the moving averages data using the Pandas Python library. 

Authors
* Marshall Presser (mpresser@pivotal.io) - Original version of this demo.
* Craig Sylvester (csylvester@pivotal.io) - Added the visualization script and made some modifications to the UPDATE statement used to add 5 and 12 month moving averages.

Changes
* May 2016 - Initial version
* June 2016 - Consolidate two UPDATEs to one. Added visualize_unemp_data.sh script
