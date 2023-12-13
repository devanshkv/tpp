import os
import yaml
import traceback
from . import database
#from . import launcher

# Read config.yml file
try:
    config_file = str(os.getenv('TPP_HOME'))+"/config.yml"
    with open(config_file, 'r') as file:
        auth = yaml.safe_load(file)
except FileNotFoundError:
    print(traceback.format_exc())
    print("BASIC TPP CODE SETUP ERROR!\n\tYou seem to not have a config.yml file in the expected\n\tplace: "+config_file+"\n\tOr, the file is for some reason unreadable.")
    exit()

# Set TPP DB authentication
tpp_user  = auth['tpp-db']['user']
tpp_pass  = auth['tpp-db']['pass']
tpp_token = auth['tpp-db']['token']
tpp_ip    = auth['tpp-db']['ip']
tpp_port  = auth['tpp-db']['port']
tpp_url = "http://" + tpp_ip + ":" + tpp_port + '/'
tpp_headers = {"Authorization": f"Bearer{tpp_token}"}

# Read globus authentication
globus_token = auth['globus']['token']
    

# Create dictionary for return.
#auth = {'tpp_user':  authentication['tpp-db']['user'],
#        'tpp_pass':  authentication['tpp-db']['pass'],
#        'tpp_token': authentication['tpp-db']['token'],
#        'tpp_ip':    authentication['tpp-db']['ip'],
#        'tpp_port':  authentication['tpp-db']['port'],
#        'tpp_url':   "http://" + tpp_ip + ":" + tpp_port + '/',
#        'tpp_headers': {"Authorization": f"Bearer{tpp_token}"},
#        'globus_token': 
#}
