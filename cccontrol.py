from optparse import OptionParser, OptionGroup

from lib.infrastructure import CCClass
from lib.cloud import CCCloud
from lib.db import CCDatabase
from lib import config
from lib import util

import logging
import sys
import os

def check_options(options):
    rc = True
    names = options.name
    optionSet = []
    optionSet.append(int(options.startClass))
    optionSet.append(int(options.query))
    optionSet.append(int(options.kill))
    optionSet.append(int(options.addCluster))
    optionSet.append(int(options.setPasswords))
    optionSet.append(int(options.configureClusters))

    if not options.database:
        logger.error('you must specify a database')
        rc = False

    if not options.name:
        logger.error('you must specify a name')
        rc = False

    if len(optionSet) <= 0:
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
            logger.error('you must specify -c and -i when starting a class')
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
                if (answer != 'Yes') and \
                   (answer != 'yes'):
                    rc = False

    if options.kill:
        print
        answer = raw_input('Are you sure you want to kill instances? ' + \
                           '(yes or no) ')
        print
        if (answer != 'Yes') and \
           (answer != 'yes'):
            rc = False
    if options.setPasswords:
        if options.passwordFile:
            options.passwordFile = os.path.expanduser(options.passwordFile)
            if not os.path.isfile(options.passwordFile):
                logger.error('password file does not exist: ' + \
                             '%s' % options.passwordFile)
                rc = False
        else:
            options.passwordFile = ''


    if options.addCluster:
        if not options.numInstances:
            logger.error('you must specify -i when adding a cluster')
            rc = False
        else:
            numInstances = int(options.numInstances)
            if numInstances > 5:
                print
                print 'You are starting %s instances.' % numInstances
                answer = raw_input('Are you sure? (yes or no) ')
                print
                if (answer != 'Yes') and \
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
    req.add_option('-n', '--name', action='store', \
                   dest='name', help='specify the name as classname ' + \
                   'or classname:clustername [REQUIRED]')
    parser.add_option_group(req)

    main = OptionGroup(parser, 'Main Options', 'You must specify one ' + \
                       'and only one of these actions.')

    main.add_option('-s', '--startClass', action='store_true', \
                    dest='startClass', help='launch a class of ' + \
                    ' instances (specify the classname and -c and -i)')
    main.add_option('-q', '--query', action='store_true', \
                    dest='query', help='query a class or cluster ')
    main.add_option('-k', '--kill', action='store_true', \
                    dest='kill', help='kill a class or cluster ')
    main.add_option('-a', '--addCluster', action='store_true', \
                    dest='addCluster', help='add a cluster to a class ' + \
                    '(specify classname and -i)')
    main.add_option('-p', '--setPasswords', action='store_true', \
                    dest='setPasswords', help='set the root passwords ' + \
                    'of a class or cluster')
    main.add_option('-l', '--configureClusters', action='store_true', \
                    dest='configureClusters', help='configure the ' + \
                    'clusters /etc/hosts file and set the hostnames ' + \
                    'of a class or cluster')
    parser.add_option_group(main)

    extra = OptionGroup(parser, 'Additional Options', 'You might need these.')

    extra.add_option('-c', '--numClusters', action='store', \
                     dest='numClusters', help='number of clusters ' + \
                     'to launch')
    extra.add_option('-i', '--numInstances', action='store', \
                     dest='numInstances', help='number of instances ' + \
                     'per cluster to launch, c * i total instances ' + \
                     'will be launched, this includes the headnode')
    extra.add_option('-f', '--passwordFile', action='store', \
                     dest='passwordFile', help='a file containing the ' + \
                     'passwords to set, in the format: clustername password')
    extra.add_option('-v', '--verbose', action='store_true', \
                     dest = 'verbose', help = 'Verbose output')
    parser.add_option_group(extra)

    parser.set_defaults(startClass=False, \
                        query=False, \
                        kill=False, \
                        addCluster=False, \
                        setPasswords=False, \
                        configureClusters=False)

    (options, args) = parser.parse_args()

    if not check_options(options):
        sys.exit(1)

    names = ''
    if options.startClass or \
       options.query or \
       options.kill or \
       options.addCluster or \
       options.setPasswords or \
       options.configureClusters:
        names = options.name

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
        ccclass.set_root_passwords(options.passwordFile)
    elif options.configureClusters:
        ccclass.configure_hosts()
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
