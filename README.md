## Greenplum DB Window Function examples

Included here are several examples of using Window functions in the Greenplum
database.

Before running the examples, run the setup.sh script to create the demo (default is `window_demo`) schema the examples use and to generate a new `00_init.sql` file. The init SQL file is referenced by all the SQL files to set the search_path (and anything else you may care to add).

File descriptions:
* `00_init.sql` : Sets a common env for all SQL scripts
* `99_clean_up.sql` : Drops the demo schema
* `setup.sh` : Creates demo schema and generates the ./00_init.sql file
* `sensor_metrics/` : Examples of how to retrieve metrics at defined time intervals
* `timeseries/` : Examples based on the [Time Series Analysis #1: Introduction to Window Functions](https://blog.pivotal.io/data-science-pivotal/products/time-series-analysis-1-introduction-to-window-functions) blog
* `unemp_rates/` : Examples of using moving averages for displaying unemployment rate data
