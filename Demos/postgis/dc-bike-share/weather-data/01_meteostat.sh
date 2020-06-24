# We are making use of the historical weather data from the Meteostat website (http://api.meteostat.net)
# After obtaining a free "key" for accessing the data, we can make simple REST api calls to retrieve
# weather data.

# Attribution

cat << EOF > attrib_meteostat.txt
Data provided by
<a href="https://www.meteostat.net" title="meteostat" target="_blank">meteostat</a>.
Meteorological data: Copyright &copy; National Oceanic and Atmospheric Administration (NOAA), Deutscher Wetterdienst (DWD).
Learn more about the <a href="https://www.meteostat.net/sources" title="meteostat Sources" target="_blank">sources</a>.
EOF

# Get an API key from MeteoStat before proceeding
echo "This demo makes use of data from a free service provided by MeteoStat. The service provide JSON API access"
echo "via a developer key requiring the user to sign up."
echo "Information is available here: https://dev.meteostat.net/getting-started"

read -p "Please enter your MeteoStat key: " mkey
[[ ! -z $mkey ]] && export METEOSTAT_KEY=${mkey} || { echo "MeteoStat access key is required."; exit 0; }

long='-77.03'
lat='38.89'
tz='America/New_York'

# To search for stations near a long/lat:
echo "Five closest weather observation stations near the Smithsonian (long/lat = $long/$lat)"
wget -q -O dca_stations_near.json "https://api.meteostat.net/v1/stations/nearby?lat=${lat}&lon=${long}&limit=5&key=${METEOSTAT_KEY}" | jq -c '.data[]'

# For our purposes, we are using the observation data from Washington National Airport (DCA)
# 72405,US,"Washington National Airport",38.8500,-77.0333,5,KDCA,72405,DCA,America/New_York

echo "Save the 'id' of the weather station you want to use and use for the remaining download requests"
echo "historical weather data"

echo "Downloading hourly observations for years 2018 and 2019"
# Hourly weather observations
id=72405
start="2018-01-01"
end="2019-12-31"
t_format="Y-m-d%20H:i:s"
# Daily weather observations
wget -q -O dca_daily_2018_2019.json "https://api.meteostat.net/v1/history/daily?station=${id}&start=${start}&end=${end}&key=${METEOSTAT_KEY}"

# Hourly weather observations
wget -q -O dca_hourly_2018_2019.json "https://api.meteostat.net/v1/history/hourly?station=${id}&start=${start}&end=${end}&time_zone=${tz}&time_format=${t_format}&key=${METEOSTAT_KEY}"
