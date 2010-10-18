import sqlite3
import logging
import os

logger = logging.getLogger('cloudcluster')

class CCDatabase:
    def __init__(self, db_filename):
        if os.path.isfile(db_filename):
            self.conn = sqlite3.connect(db_filename)
            self.dbh = self.conn.cursor()
        else:
            self.conn = sqlite3.connect(db_filename)
            self.dbh = self.conn.cursor()
            self.dbh.execute('''create table class
                 (id integer primary key, 
                 name text)''')
            self.dbh.execute('''create table cluster
                 (id integer primary key, 
                 count integer, 
                 class_id integer foreign_key references class(id))''')
            self.dbh.execute('''create table instance
                 (id integer primary key,
                 name text,
                 node_name text,
                 ip_address text,
                 private_ip text,
                 password text,
                 headnode integer,
                 state integer,
                 cluster_id integer foreign_key references cluster(id))''')
            self.conn.commit()

    def close(self):
        try:
            self.conn.commit()
            self.dbh.close()
        except:
            logger.error('problem closing database')

    def print_db(self):
        select_class_str = 'select * from class'
        select_cluster_str = 'select * from cluster'
        select_instance_str = 'select * from instance'
        print
        print 'table: class'
        self.dbh.execute(select_class_str)
        for row in self.dbh:
            print row
        print 'table: cluster'
        self.dbh.execute(select_cluster_str)
        for row in self.dbh:
            print row
        print 'table: instance'
        self.dbh.execute(select_instance_str)
        for row in self.dbh:
            print row
        print

    def is_class_in_db(self, name):
        self.dbh.execute('select * from class where name=?', (name,))
        for row in self.dbh:
            if row != []:
                return True
        return False

    def get_class_id(self, class_name):
        select_str = 'select id from class where name=?'
        item = (class_name,)
        self.dbh.execute(select_str, item)
        class_id = -1
        for row in self.dbh:
            class_id = row[0]
        return class_id

    def get_cluster_id(self, class_name, cluster_name):
        class_id = self.get_class_id(class_name)
        cluster_count = int(cluster_name.lstrip('cluster'))
        select_str = 'select id from cluster where class_id=? and count=?'
        item = (class_id, cluster_count)
        self.dbh.execute(select_str, item)
        cluster_id = -1
        for row in self.dbh:
            cluster_id = row[0]
        return cluster_id

    def get_cluster_ids(self, class_id):
        select_str = 'select id from cluster where class_id=?'
        item = (class_id,)
        self.dbh.execute(select_str, item)
        cluster_ids = []
        for row in self.dbh:
            cluster_ids.append(row[0])
        return cluster_ids

    def get_all_cluster_counts(self, class_name):
        class_id = self.get_class_id(class_name)
        select_str = 'select count from cluster where class_id=?'
        item = (class_id,)
        self.dbh.execute(select_str, item)
        all_counts = []
        for row in self.dbh:
            all_counts.append(row[0])
        return all_counts

    def get_new_cluster_count(self, class_name):
        all_counts = self.get_all_cluster_counts(class_name)
        if all_counts == []:
            return 0
        return len(all_counts) 

    def get_all_node_names(self, class_name):
        class_id = self.get_class_id(class_name)
        cluster_ids = self.get_cluster_ids(class_id)
        select_str = 'select node_name from instance where cluster_id=?'
        node_names = []
        for cluster_id in cluster_ids:
            item = (cluster_id,)
            self.dbh.execute(select_str, item)
            for row in self.dbh:
                node_names.append(row[0])
        return node_names

    def get_all_cluster_node_names(self, class_name, cluster_name):
        cluster_id = self.get_cluster_id(class_name, cluster_name)
        select_str = 'select node_name from instance where cluster_id=?'
        item = (cluster_id,)
        self.dbh.execute(select_str, item)
        node_names = []
        for row in self.dbh:
            node_names.append(row[0])
        return node_names

    def add_class(self, name):
        if not self.is_class_in_db(name):
            item = (None, name)
            self.dbh.execute('insert into class values (?, ?)', item)

    def add_cluster(self, class_name, count):
        class_id = self.get_class_id(class_name)
        insert_str = 'insert into cluster values (?, ?, ?)'
        item = (None, count, class_id)
        self.dbh.execute(insert_str, item)

    def add_instance(self, \
                     class_name, \
                     cluster_name, \
                     name, \
                     node_name, \
                     headnode, \
                     state):
        cluster_id = self.get_cluster_id(class_name, cluster_name)
        insert_str = 'insert into instance values ' + \
                     '(?, ?, ?, ?, ?, ?, ?, ?, ?)'
        item = (None, \
                name, \
                node_name, \
                '', \
                '', \
                '', \
                headnode, \
                state, \
                cluster_id)
        self.dbh.execute(insert_str, item)

    def set_instance_state(self, node_name, state):
        update_str = 'update instance set state=? where node_name=?'
        item = (state, node_name)
        self.dbh.execute(update_str, item)

    def set_instance_password(self, ip_address, password):
        update_str = 'update instance set password=? where ip_address=?'
        item = (password, ip_address)
        self.dbh.execute(update_str, item)

    def set_instance_ip_address(self, node_name, ip_address, private_ip=''):
        update_str = 'update instance set ip_address=? where node_name=?'
        item = (ip_address, node_name)
        self.dbh.execute(update_str, item)
        update_str = 'update instance set private_ip=? where node_name=?'
        item = (private_ip, node_name)
        self.dbh.execute(update_str, item)

    def get_instance_name(self, node_name):
        select_str = 'select name from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        name = ''
        for row in self.dbh:
            name = row[0]
        return name

    def get_instance_password(self, node_name):
        select_str = 'select password from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        password = ''
        for row in self.dbh:
            password = row[0]
        return password

    def get_instance_state(self, node_name):
        select_str = 'select state from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        state = ''
        for row in self.dbh:
            state = row[0]
        return state

    def get_instance_headnode(self, node_name):
        select_str = 'select headnode from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        headnode = -1
        for row in self.dbh:
            headnode = row[0]
        return headnode

    def get_instance_cluster_name(self, node_name):
        select_str = 'select cluster_id from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        cluster_id = -1
        for row in self.dbh:
            cluster_id = row[0]
        select_str = 'select count from cluster where id=?'
        item = (cluster_id,)
        self.dbh.execute(select_str, item)
        cluster_count = -1
        for row in self.dbh:
            cluster_count = row[0]
        return 'cluster' + str(cluster_count)

    def get_instance_ip_address(self, node_name):
        select_str = 'select ip_address from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        ip_address = ''
        for row in self.dbh:
            ip_address = row[0]
        return ip_address 

    def get_instance_private_ip(self, node_name):
        select_str = 'select private_ip from instance where node_name=?'
        item = (node_name,)
        self.dbh.execute(select_str, item)
        private_ip = ''
        for row in self.dbh:
            private_ip = row[0]
        return private_ip
