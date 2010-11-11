import logging
import sys
import os

logger = logging.getLogger('cloudcluster')

def configure_logger():
    formatter = logging.Formatter('%(asctime)s - %(name)-14s - ' + \
                                  '%(levelname)-8s - %(message)s')
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logging.getLogger('').setLevel(logging.INFO)

def check_config(config_vals):
    rc = True

    try:
        tmp = config_vals['CLOUD']
        if tmp != 'amazon':
            logger.error('amazon is the only supported cloud')
            rc = False
    except:
        logger.error('CLOUD not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['LOCATION']
        if (tmp != 'EC2_US_EAST') and \
           (tmp != 'EC2_US_WEST'):
            logger.error('Location must be EC2_US_EAST or EC2_US_WEST')
            rc = False
    except:
        logger.error('LOCATION not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['CLOUD_IMAGE']
    except:
        logger.error('CLOUD_IMAGE not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['CLOUD_SIZE']
    except:
        logger.error('CLOUD_SIZE not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['KEYPAIR_NAME']
    except:
        logger.error('KEYPAIR_NAME not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['KEYPAIR_FILE']
        if not os.path.isfile(config_vals['KEYPAIR_FILE']):
            logger.error('KEYPAIR_FILE does not exist: ' + \
                         '%s' % config_vals['KEYPAIR_FILE'])
            rc = False
    except:
        logger.error('KEYPAIR_FILE not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['ACCESS_ID_FILE']
        if not os.path.isfile(config_vals['ACCESS_ID_FILE']):
            logger.error('ACCESS_ID_FILE does not exist: ' + \
                         '%s' % config_vals['ACCESS_ID_FILE'])
            rc = False
    except:
        logger.error('ACCESS_ID_FILE not set in cloudcluster.conf')
        rc = False

    try:
        tmp = config_vals['SECRET_KEY_FILE']
        if not os.path.isfile(config_vals['SECRET_KEY_FILE']):
            logger.error('SECRET_KEY_FILE does not exist: ' + \
                         '%s' % config_vals['SECRET_KEY_FILE'])
            rc = False 
    except:
        logger.error('SECRET_KEY_FILE not set in cloudcluster.conf')
        rc = False

    return rc

def read_config():
    config_vals = {}

    config_filename = './cloudcluster.conf'
    config_file = open(config_filename, 'r')
    config_lines = config_file.readlines()
    config_file.close()

    for line in config_lines:
        if (not line.startswith('#')) and \
           (line.strip() != ''):
            splitline = line.split()
            key = splitline[0].strip()
            value = splitline[2].strip()
            if key == 'ACCESS_ID_FILE':
                value = os.path.expanduser(value)
            if key == 'SECRET_KEY_FILE':
                value = os.path.expanduser(value)
            if key == 'KEYPAIR_FILE':
                value = os.path.expanduser(value)
            config_vals[key] = value

    if not check_config(config_vals):
        logger.error('cloudcluster not configured properly')
        sys.exit(1)

    logger.debug('read configuration: %s' % config_vals)

    return config_vals
