from libcloud.types import Provider
from libcloud.providers import get_driver
from lib import util

import logging
import time
import sys

logger = logging.getLogger('cloudcluster')

class CCCloud:
    def __init__(self, config_vals):
        self.accessid = util.read_line(config_vals['ACCESS_ID_FILE'])
        self.secretkey = util.read_line(config_vals['SECRET_KEY_FILE'])
        self.keypair_name = config_vals['KEYPAIR_NAME']
        self.keypair_file = config_vals['KEYPAIR_FILE']
        self.set_cloud_conn(config_vals['CLOUD'])
        self.set_image(config_vals['CLOUD_IMAGE'])
        self.set_size(config_vals['CLOUD_SIZE'])

    def set_size(self, cloudSize):
        self.size = ''
        sizes = self.cloud_conn.list_sizes()
        for asize in sizes:
            if asize.id == cloudSize:
                self.size = asize

    def set_image(self, cloudImage):
        self.image = ''
        images = self.cloud_conn.list_images()
        for aimage in images:
            if aimage.id == cloudImage:
                self.image = aimage

    def set_cloud_conn(self, cloud):
        self.cloud_conn = None
        if cloud == 'amazon':
            driver = get_driver(Provider.EC2)
            self.cloud_conn = driver(self.accessid, self.secretkey)
        else:
            logger.error('failed to set a cloud driver')
            sys.exit(1)

    def get_all_instances(self):
        return self.cloud_conn.list_nodes()

    def get_instance(self, name):
        all_instances = self.get_all_instances()
        for instance in all_instances:
            if instance.name == name:
                return instance

    def kill_node(self, name):
        instance = self.get_instance(name)
        try:
            self.cloud_conn.destroy_node(instance)
        except:
            logger.error('problem killing node %s' % name)

    def launch_node(self, name):
        node = self.cloud_conn.create_node(name=name, \
                                           image=self.image, \
                                           size=self.size, \
                                           ex_keyname=self.keypair_name)
        return node.name, node.state
