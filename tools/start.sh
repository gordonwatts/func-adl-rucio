# Startup the rucio downlaoder stuffs
#
# We do need a few arguments:
#  start.sh <dataset-location> <rabbit-mq-address> <rabbit-mq-username> <rabbit-mq-password> <rucio-username> <rucio_voms> <cert-password>

diskloc=$1
xcache=$2
rabbitmq_addr=$3
rabbitmq_user=$4
rabbitmq_pass=$5

rucio_username=$6
rucio_voms=$7
cert_pass=$8

# Get the certificate manager up and running
export GRID_VOMS=${!rucio_voms:=$rucio_voms}
export GRID_PASSWORD=${!cert_pass:=$cert_pass}
export RUCIO_ACCOUNT=${!rucio_username:=$rucio_username}

python3 cert_manager.py &

# Give the system a chance to grab the first cert so we don't go into backoff mode.
sleep 15

# Next, get the rabbit mq powered downloader up and going.
python3 rucio_by_rabbit.py $diskloc $xcache $rabbitmq_addr $rabbitmq_user $rabbitmq_pass