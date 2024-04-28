!/bin/bash

# This is to link the SQLite file with Grafana volume
chown -R 472:472  /opt/gno/data
chmod -R 775 /opt/gno/data
groupadd -g 472 grafanagroup
useradd -u 472 -g 472 -m grafanauser
# End Grafana-permissions setup
/bin/sudo  -u grafanauser enginetwo