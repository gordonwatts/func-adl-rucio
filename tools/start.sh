# Startup the rucio downlaoder stuffs
#
# We do need a few arguments:
#  start.sh <dataset-location> <rabbit-mq-address> <rabbit-mq-username> <rabbit-mq-password> <rucio-username> <rucio_voms> <cert-password>

diskloc=$1
rabbitmq_addr=$2
rabbitmq_user=$3
rabbitmq_pass=$4

rucio_username=$5
rucio_voms=$6
cert_pass=$7

# Get the certificate manager up and running
export GRID_VOMS=$rucio_voms
export GRID_PASSWORD=$cert_pass
export RUCIO_ACCOUNT=$rucio_username

python3 cert_manager.py &

# Give the system a chance to grab the first cert so we don't go into backoff mode.
sleep 15

# Next, get the rabbit mq powered downloader up and going.
python3 rucio_by_rabbit.py $diskloc $rabbitmq_addr $rabbitmq_user $rabbitmq_pass