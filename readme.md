### Instructions
#### Go to folder `influx`:
`cd $HOME/influx/`

#### Get from repo:
```
git clone https://github.com/RaulBernal/enginetwo.git
cd enginetwo
go run main.go
```

#### Query the InfluxDB to check insertions
List all indexed blocks 
```
curl -s --get "$INFLUXDB_URL/query" \
  --user $INFLUXDB_USERNAME:$INFLUXDB_TOKEN \
  --data-urlencode "db=gnoland_spacecraft" \
  --data-urlencode "q=SELECT * FROM blocks2" |jq
```