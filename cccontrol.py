from optparse import OptionParser, OptionGroup

from lib.infrastructure import CCClass
from lib.cloud import CCCloud
from lib.db import CCDatabase
from lib import config
from lib import util

import logging
import sys

def check_options(options):
    rc = True
    optionSet = [0]
    if options.startClass:
        names = options.startClass
        optionSet.append(1)
    if options.query:
        names = options.query
        optionSet.append(1)
    if options.kill:
        names = options.kill
        optionSet.append(1)
    if options.addCluster:
        names = options.addCluster
        optionSet.append(1)
    if options.setPasswords:
        names = options.setPasswords
        optionSet.append(1)
    if options.setupClusterHosts:
        names = options.setupClusterHosts
        optionSet.append(1)

    if len(optionSet) <= 1:
        logger.error('you must specify at least one main action')
        rc = False
    else:
        if not reduce(lambda x, y: x^y, optionSet):
            logger.error('you can only specify one main action')
            rc = False

    if names == '':
        logger.error('you must specify a classname or classname ' + \
                     ':clustername')
        rc = False
    else:
        splitnames = names.split(':')
        if len(splitnames) > 3:
            logger.error('bad argument: %s' % names)
            rc = False

    if options.startClass:
        if not (options.numInstances and options.numClusters):
            logger.error('you must specify -n and -m when starting a class')
            rc = False
        else:
            numInstances = int(options.numInstances)
            numClusters = int(options.numClusters)
            total = numInstances * numClusters
            if total > 5:
                print
                print 'You are starting %s instances.' % total
                answer = raw_input('Are you sure? (yes or no) ')
                print
                if (answer != 'Yes') or \
                   (answer != 'yes'):
                    rc = False

    if options.addCluster:
        if not options.numInstances:
            logger.error('you must specify -n when adding a cluster')
            rc = False
        else:
            numInstances = int(options.numInstances)
            if numInstances > 5:
                print
                print 'You are starting %s instances.' % numInstances
                answer = raw_input('Are you sure? (yes or no) ')
                print
                if (answer != 'Yes') or \
                   (answer != 'yes'):
                    rc = False

    return rc

def parse_options():
    parser = OptionParser()

    req = OptionGroup(parser, 'Required Options', 'You have to use these.')

    req.add_option('-d', '--database', action='store', \
                   dest='database', help='location of the ' + \
                   'sqlite database file to use (this file will be ' + \
                   'created if it does not exist) [REQUIRED]')
    parser.add_option_group(req)

    main = OptionGroup(parser, 'Main Options', 'You must specify one ' + \
                       'and only one of these actions.')

    main.add_option('-s', '--startClass', action='store', \
                    dest='startClass', help='launch a class of ' + \
                    ' instances (specify classname and include -m and -n)')
    main.add_option('-q', '--query', action='store', \
                    dest='query', help='query a class or cluster ' + \
                    '(specify either classname or classname:' + \
                    'clustername)')
    main.add_option('-k', '--kill', action='store', \
                    dest='kill', help='kill a class or cluster ' + \
                    '(specify either classname or classname:' + \
                    'clustername)')
    main.add_option('-a', '--addCluster', action='store', \
                    dest='addCluster', help='add a cluster to a class ' + \
                    '(specify classname and include -n)')
    main.add_option('-p', '--setPasswords', action='store', \
                    dest='setPasswords', help='set the root passwords ' + \
                    '(specify either classname or classname:' + \
                    'clustername or classname:clustername:' + \
                    'instancename)')
    main.add_option('-c', '--setupClusterHosts', action='store', \
                    dest='setupClusterHosts', help='configure the ' + \
                    'clusters /etc/hosts file (specify either ' + \
                    'classname or classname:clustername)')
    parser.add_option_group(main)

    extra = OptionGroup(parser, 'Additional Options', 'You might need these.')

    extra.add_option('-m', '--numClusters', action='store', \
                     dest='numClusters', help='number of clusters ' + \
                     'to launch')
    extra.add_option('-n', '--numInstances', action='store', \
                     dest='numInstances', help='number of instances ' + \
                     'per cluster to launch, m * n total instances ' + \
                     'will be launched, this includes the headnode')
    extra.add_option('-v', '--verbose', action='store_true', \
                     dest = 'verbose', help = 'Verbose output')
    parser.add_option_group(extra)

    (options, args) = parser.parse_args()

    if not check_options(options):
        sys.exit(1)

    names = ''
    if options.startClass:
        names = options.startClass
    elif options.query:
        names = options.query
    elif options.kill:
        names = options.kill
    elif options.addCluster:
        names = options.addCluster
    elif options.setPasswords:
        names = options.setPasswords
    elif options.setupClusterHosts:
        names = options.setupClusterHosts

    if options.verbose:
        logging.getLogger('').setLevel(logging.DEBUG)

    return options, names

def main():
    rc = 0
    options, names = parse_options()
    config_vals = config.read_config()
    class_name, cluster_name, instance_name = util.get_names(names)

    cccloud = CCCloud(config_vals)
    db = CCDatabase(options.database)

    logger.debug('db before command:')
    if options.verbose:
        db.print_db()

    ccclass = CCClass(class_name, cluster_name, instance_name, db, cccloud)

    if options.startClass:
        numClusters = int(options.numClusters)
        numInstances = int(options.numInstances)
        ccclass.launch(numClusters, numInstances)
    elif options.query:
        ccclass.query()
    elif options.addCluster:
        numInstances = int(options.numInstances)
        ccclass.deploy_cluster(numInstances)
    elif options.kill:
        ccclass.kill()
    elif options.setPasswords:
        ccclass.set_root_passwords()
    elif options.setupClusterHosts:
        ccclass.setup_cluster_hosts()
    else:
        logger.error('you must specify one action')
        return 1

    logger.debug('db after command:')
    if options.verbose:
        db.print_db()

    db.close()

    return rc

if __name__ == '__main__':
    config.configure_logger()
    logger = logging.getLogger('cloudcluster')
    rc = main()
    sys.exit(rc)
