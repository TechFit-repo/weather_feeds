DIR='/path/to/your/repo/weather_feeds'
LOGFILE="$DIR/log"
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
echo "$TIMESTAMP - Starting BASH script ->" >> $LOGFILE/bash.log
echo "$TIMESTAMP - Starting BASH script ->"

# Run the script
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
echo "$TIMESTAMP - Running Python Service (ETL) ->" >> $LOGFILE/bash.log
echo "$TIMESTAMP - Running Python Service (ETL) ->"

echo " "
echo " "

cd "$DIR"
/opt/anaconda3/bin/python -m src.weather_feeds_api

echo " "
echo " "

TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
echo "$TIMESTAMP - Finishing BASH script..." >> $LOGFILE/bash.log
echo "$TIMESTAMP - Finishing BASH script..."
