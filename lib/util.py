import telnetlib
import random
import string
import logging
import time

logger = logging.getLogger('cloudcluster')

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

def read_password_file(filename):
    theFile = open(filename, 'r')
    lines = theFile.readlines()
    theFile.close()
    passwords = {}
    for line in lines:
        splitline = line.split()
        try:
            cluster_name = splitline[0].strip()
            password = splitline[1].strip()
            passwords[cluster_name] = password
        except:
            logger.debug('problem splitting password line: %s' % line)
    logger.debug('read passwords: %s' % passwords)
    return passwords

def generate_random_password(length=8):
    chars = string.letters + string.digits 
    symbols = '!@#%^&*'
    password = ''
    for i in range(2):
        password = password + random.choice(chars)
    for i in range(length-2):
        password = password + random.choice(chars + symbols)
    return password

def is_port_open(hostname, port, expected='', timeout=2):
    rc = False
    num_tries = 1
    while num_tries < 4:
        logger.debug('Try %s: checking %s:%s' % (num_tries, hostname, port))
        try:
            logger.debug('Telneting to host %s on port %s' % (hostname, port))
            teln = telnetlib.Telnet(hostname, port, timeout)
            msg = teln.read_until(expected, timeout)
            logger.debug('Connected to ' + \
                         '%s:%s and read %s' % (hostname, port, msg))
            logger.debug('Message length: %d' % len(msg))
            if len(msg) <= 0:
                rc = False
                errorStr = 'We connected to port %s but nothing ' + \
                           'was there'
                errorStr = errorStr % port
                logger.error(errorStr)
            else:
                logger.debug('%s:%s is up' % (hostname, port))
                num_tries = 4
                rc = True
        except:
            rc = False
            errorStr = 'Host %s does not appear to be listening on port %s.'
            errorStr = errorStr % (hostname, port)
            logger.error(errorStr)
        if rc == False:
            time.sleep(2)
        num_tries += 1
    return rc
