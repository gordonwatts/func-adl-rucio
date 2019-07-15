# func-adl-rucio
 Simple rucio container that will download files on rabbitmq demand.

# cert_manager.py

This is a loop that will run and keep your ATLAS cert valid, renewing every 11 hours or so.
If you are disconnected from the internet, it will retry every 5 minutes until it successfully
gets re-authorized.

For this to run a few environment variables need to be defined:

    `GRID_PASSWORD`     The password to access your certificate
    `GRID_VOMS`         The virtual organization for your GRID cert

Further, you'll need a certificate. It can be mounted in one of two places:

    `/certs`            Mount your certs here if you are on a windows system (or want to mount readonly)
    `/root/.globus`     Mount your certs here if you are on a linux system

The script will `chmod` your certificates to make sure they are ready for use by making sure they have
the correct permission settings. If you mount in `/root/.globus`, then mount read/write, and further, do
not be on a filesystem (e.g. windows) that can't support the Linux permission bits.