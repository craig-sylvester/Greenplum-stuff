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
tz='America%2FNew_York'

API_BASE=https://meteostat.p.rapidapi.com
# WGET Parameters
# " --quiet --header='x-rapidapi-host: meteostat.p.rapidapi.com' --header='x-rapidapi-key: ${METEOSTAT_KEY}' "
#--header='x-rapidapi-key: 723291a955msh7cb96da6277e4c4p12d063jsnf44ed5ccfa3e' "

# To search for stations near a long/lat:
echo "Five closest weather observation stations near the Smithsonian (long/lat = $long/$lat)"
wget -O dca_stations_near.json --quiet --header="x-rapidapi-host: meteostat.p.rapidapi.com" --header="x-rapidapi-key: ${METEOSTAT_KEY}" "${API_BASE}/stations/nearby?lat=${lat}&lon=${long}&limit=5" | jq -c '.data[]'

# For our purposes, we are using the observation data from Washington National Airport (DCA)
# 72405,US,"Washington National Airport",38.8500,-77.0333,5,KDCA,72405,DCA,America/New_York

echo "Save the 'id' of the weather station you want to use and use for the remaining download requests"
echo "historical weather data"

echo "Downloading daily observations for years 2018 and 2019"
id=72405
start="2018-01-01"
end="2019-12-31"
t_format="Y-m-d%20H:i:s"
# Daily weather observations
wget -O dca_daily_2018_2019.json  --quiet --header="x-rapidapi-host:meteostat.p.rapidapi.com" --header="x-rapidapi-key:${METEOSTAT_KEY}" "${API_BASE}/stations/daily?station=${id}&start=${start}&end=${end}"

echo "Downloading hourly observations for years 2018 and 2019"
# Hourly weather observations
# There is a limit of 30 days for retrieving hourly data:
# https://dev.meteostat.net/api/stations/hourly.html
days_in_month=(31 28 31 30 31 30 31 31 30 31 30 31)

hourly_file="dca_hourly_2018_2019.json"
touch ${hourly_file}

for yr in 2018 2019
do
    for mth in $(seq -w 01 12)
    do
	m=$(( $(printf "%d" ${mth#0}) - 1 ))
        wget -O wget.out  --quiet --header=x-rapidapi-host:meteostat.p.rapidapi.com --header=x-rapidapi-key:${METEOSTAT_KEY} "${API_BASE}/stations/hourly?station=${id}&start=${yr}-${mth}-01&end=${yr}-${mth}-${days_in_month[$m]}&time_zone=${tz}"
	sleep 0.5

        cat wget.out >> ${hourly_file}
    done
done

rm wget.out

exit 0
