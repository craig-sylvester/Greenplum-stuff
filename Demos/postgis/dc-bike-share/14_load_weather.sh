#!/usr/bin/env bash

source ./dcbikeshare_variables.sh
set -eu

${weather_dir}/01_meteostat.sh

${weather_dir}/02_load_weather_data.sh
