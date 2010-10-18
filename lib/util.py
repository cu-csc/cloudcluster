import random
import string

def node_state_tostring(state):
    if state == 0:
        return 'RUNNING'
    elif state == 1:
        return 'REBOOTING'
    elif state == 2:
        return 'TERMINATED'
    elif state == 3:
        return 'PENDING'
    elif state == 4:
        return 'UNKNOWN'
    else:
        return 'BADSTATE'

def headnode_tostring(headnode):
    if headnode:
        return 'Yes'
    else:
        return 'No'

def get_names(names):
    class_name = names.split(':')[0]
    try:
        cluster_name = names.split(':')[1]
    except:
        cluster_name = ''
    try:
        instance_name = names.split(':')[2]
    except:
        instance_name = ''
    return class_name, cluster_name, instance_name

def read_line(filename):
    theFile = open(filename, 'r')
    thevalue = theFile.readline()
    theFile.close()
    return thevalue.strip()

def generate_random_password(length=8):
    chars = string.letters + string.digits 
    symbols = '!@#$%^&*'
    password = ''
    for i in range(2):
        password = password + random.choice(chars)
    for i in range(length-2):
        password = password + random.choice(chars + symbols)
    return password
