from libcloud.types import NodeState
from lib import util

import commands
import logging

logger = logging.getLogger('cloudcluster')

class CCClass:
    def __init__(self, name, cluster_name, instance_name, db, cccloud):
        self.name = name
        self.cluster_name = cluster_name
        self.instance_name = instance_name
        self.db = db
        self.cccloud = cccloud
        self.db.add_class(name)

    def print_node(self, node_name):
        name = self.db.get_instance_name(node_name)
        password = self.db.get_instance_password(node_name)
        headnode = self.db.get_instance_headnode(node_name)
        state = self.db.get_instance_state(node_name)
        ip_address = self.db.get_instance_ip_address(node_name)
        private_ip = self.db.get_instance_private_ip(node_name)
        print '  Node:  ' 
        print '\tName:\t\t%s' % name
        print '\tInstance:\t%s' % node_name
        print '\tIP Address:\t%s (external)' % ip_address
        print '\tIP Address:\t%s (internal)' % private_ip
        print '\tPassword:\t%s' % password
        print '\tState:\t\t%s' % util.node_state_tostring(state)
        print '\tHeadnode:\t%s' % util.headnode_tostring(headnode)

    def print_info(self):
        print 'class: %s' % self.name
        if self.cluster_name != '':
            print 'cluster: %s' % self.cluster_name
            node_names = self.db.get_all_cluster_node_names(self.name, \
                                                            self.cluster_name)
            for node_name in node_names:
                self.print_node(node_name)
        else:
            cluster_counts = self.db.get_all_cluster_counts(self.name)
            cluster_counts.sort()

            for cluster_count in cluster_counts:
                cluster_name = 'cluster%02d' % cluster_count
                print 'cluster: %s' % cluster_name
                node_names = self.db.get_all_cluster_node_names(self.name, \
                                                                cluster_name)
                for node_name in node_names:
                    self.print_node(node_name)
        print

    def launch(self, clusters=0, instances=0):
        for i in range(clusters):
            self.deploy_cluster(instances)

    def query(self, print_info=True):
        if self.cluster_name == '':
            node_names = self.db.get_all_node_names(self.name)
        else:
            node_names = self.db.get_all_cluster_node_names(self.name, \
                                                            self.cluster_name)

        for node_name in node_names:
            instance = self.cccloud.get_instance(node_name)
            self.db.set_instance_state(node_name, instance.state)
            self.db.set_instance_ip_address(node_name, \
                                            instance.public_ip[0], \
                                            instance.private_ip[0])

        if print_info:
            self.print_info()

    def kill(self):
        if self.cluster_name == '':
            node_names = self.db.get_all_node_names(self.name)
        else:
            node_names = self.db.get_all_cluster_node_names(self.name, \
                                                            self.cluster_name)
        for node_name in node_names:
            logger.info('killing node %s' % node_name)
            self.cccloud.kill_node(node_name)
            self.db.set_instance_state(node_name, NodeState.TERMINATED)

    def configure_cluster(self, node_names):
        for host_name in node_names:
            ip = self.db.get_instance_ip_address(host_name)
            name_to_set = self.db.get_instance_name(host_name)
            logger.info('Configuring host %s' % ip)
            etc_hosts = ''
            for node_name in node_names:
                name = self.db.get_instance_name(node_name)
                private_ip = self.db.get_instance_private_ip(node_name)
                app_str = 'ssh -o \'StrictHostKeyChecking no\' -i %s ' + \
                          'root@%s \"echo %s %s >> /etc/hosts %s\"'
                in_str = '\`host %s | awk \'{print \$4}\'\`' % private_ip
                last_str = ';hostname %s' % name_to_set
                ssh_str = app_str % (self.cccloud.keypair_file, \
                                     ip, \
                                     in_str, \
                                     name, \
                                     last_str)
                logger.debug(ssh_str)
                (status, output) = commands.getstatusoutput(ssh_str)
                if status != 0:
                    logger.error('Problem configuring ' + \
                                 '%s' % ip)
                    print output

    def configure_hosts(self):
        self.query(False) 
        node_names = []
        if self.cluster_name != '':
            node_names = self.db.get_all_cluster_node_names(self.name, \
                                                            self.cluster_name)
            self.configure_cluster(node_names)
        else:
            cluster_counts = self.db.get_all_cluster_counts(self.name)
            for cluster_count in cluster_counts:
                cluster_name = 'cluster%02d' % cluster_count
                node_names = self.db.get_all_cluster_node_names(self.name, \
                                                                cluster_name)
                self.configure_cluster(node_names)

    def deploy_cluster(self, instances=0):
        count = self.db.get_new_cluster_count(self.name)
        self.db.add_cluster(self.name, count)
        cluster_name = 'cluster%02d' % count
        logger.info('launching new cluster %s:%s' % (self.name, cluster_name))
        new_nodes = self.cccloud.launch_nodes(cluster_name, instances)
        if instances == 1:
            nodes = [new_nodes]
        else:
            nodes = new_nodes
        i = 0
        for node in nodes:
            if i == 0:
                headnode = 1
                name = '%s-head' % cluster_name
            else:
                headnode = 0
                name = '%s-node%02d' % (cluster_name, i)
            node_name = node.name
            state = node.state
            self.db.add_instance(self.name, \
                                 cluster_name, \
                                 name, \
                                 node_name, \
                                 headnode, \
                                 state)
            i += 1

    def set_root_passwords(self, passwordFile=''):
        chpasswdStr = 'ssh -o \'StrictHostKeyChecking no\' -i %s ' + \
                      'root@%s \'echo root:%s | chpasswd\''
        self.query(False) 

        passwords = {}
        if passwordFile != '':
            logger.debug('reading password file: %s' % passwordFile)
            passwords = util.read_password_file(passwordFile)

        if self.instance_name != '':
            node_names = [self.instance_name]
        elif self.cluster_name != '':
            node_names = self.db.get_all_cluster_node_names(self.name, \
                                                            self.cluster_name)
        else:
            node_names = self.db.get_all_node_names(self.name)

        cluster_counts = self.db.get_all_cluster_counts(self.name)
        for cluster_count in cluster_counts:
            cluster_name = 'cluster%02d' % cluster_count
            cluster_nodes = self.db.get_all_cluster_node_names(self.name, \
                                                               cluster_name)
            try:
                logger.debug('getting cluster password for: ' + \
                             '%s' % cluster_name)
                new_password = passwords[cluster_name]
            except:
                logger.debug('failed to get a password, generating a ' + \
                             'random password instead')
                new_password = util.generate_random_password()
            for node_name in cluster_nodes:
                password = self.db.get_instance_password(node_name)
                if password == '':
                    ip = self.db.get_instance_ip_address(node_name)
                    self.db.set_instance_password(ip, new_password)

        for node_name in node_names:
            state = self.db.get_instance_state(node_name)
            if state == NodeState.RUNNING:
                ip = self.db.get_instance_ip_address(node_name)
                password = self.db.get_instance_password(node_name)
                setStr = chpasswdStr % (self.cccloud.keypair_file, ip, password)
                logger.debug('EXE: %s' % setStr)
                logger.info('Setting %s with password %s' % (ip, password))
                (status, output) = commands.getstatusoutput(setStr)
                if status != 0:
                    logger.error('Problem setting password')
                    print output
                else:
                    self.db.set_instance_password(ip, password)
