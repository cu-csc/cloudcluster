from lib import util
from lib import config

from optparse import OptionParser, OptionGroup

import logging
import sys

def check_options(options):
    rc = True
    if not options.numClusters:
        logger.error('you must specify the number of clusters')
        rc = False

    return rc

def parse_options():
    parser = OptionParser()

    req = OptionGroup(parser, 'Required Options', 'You have to use these.')

    req.add_option('-c', '--numClusters', action='store', \
                   dest='numClusters', type='int',
                   help='number of clusters ' + \
                   'for which to generate passwords [REQUIRED]')
    parser.add_option_group(req)

    parser.set_defaults(numClusters=False)

    (options, args) = parser.parse_args()

    if not check_options(options):
        sys.exit(1)

    return options, args

def main():
    rc = 0
    options, args = parse_options()

    for cluster in range(options.numClusters):
        new_password = util.generate_random_password()
        print 'cluster%02d %s' % (cluster, new_password)
    
if __name__ == '__main__':
    config.configure_logger()
    logger = logging.getLogger('genpasswords')
    rc = main()
    sys.exit(rc)
