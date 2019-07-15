# Main program to keep a grid certification up to date.
from ruciopylib.cert import cert
from retry import retry
import os
import shutil
import logging
    

def copy_if_exists(source, dest):
    'Copy a file if it exists'
    if os.path.exists(source):
        shutil.copyfile(source, dest)

@retry(delay=5*60)
def setup_certs():
    '''Locate a cert and copy it over, and get it ready for use.
    We just hang if we can't find certs to use.
    '''
    globus_directory = os.path.expanduser('~/.globus')
    if not os.path.exists(globus_directory):
        logging.info(f'Creating the {globus_directory} directory')
        os.mkdir(globus_directory)
    
    # First, see if they exist in certs
    copy_if_exists('/certs/userkey.pem', f'{globus_directory}/userkey.pem')
    copy_if_exists('/certs/usercert.pem', f'{globus_directory}/usercert.pem')

    # Next, in the globus area, make sure the permissions are right.
    os.chmod(f'{globus_directory}/userkey.pem', 0o400)
    os.chmod(f'{globus_directory}/usercert.pem', 0o444)

def do_cert():
    logging.info('Setting up certs for use')
    setup_certs()
    logging.info ("Starting registration loop")
    c = cert()
    c.run_registration_loop(log_func=lambda l: logging.info(l))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    do_cert()
