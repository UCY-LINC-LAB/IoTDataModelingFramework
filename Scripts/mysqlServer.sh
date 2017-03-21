#!/bin/bash
sed -i 's/127.0.0.1 localhost/127.0.0.1 localhost manager\n172.16.4.15 gmoles1\n172.16.4.16 gmoles2\n172.16.4.18 gmoles3\n172.16.4.21 gmoles4/g' /etc/hosts
sudo apt-get install libaio1
wget https://dev.mysql.com/get/Downloads/MySQL-Cluster-7.5/mysql-cluster-gpl-7.5.5-debian8-x86_64.deb
sudo dpkg -i mysql-cluster-gpl-7.5.5-debian8-x86_64.deb
sudo mkdir /var/lib/mysql-cluster
sudo touch /var/lib/mysql-cluster/config.ini
echo "
[ndb_mgmd]
# Management process options:
hostname=gmoles1  # Hostname of the manager
datadir=/var/lib/mysql-cluster  # Directory for the log files

[ndbd]
hostname=gmoles1   # Hostname of the first data node
datadir=/usr/local/mysql/data   # Remote directory for the data files

[ndbd]
hostname=gmoles2   # Hostname of the second data node
datadir=/usr/local/mysql/data   # Remote directory for the data files

[ndbd]
hostname=gmoles3    # Hostname of the second data node
datadir=/usr/local/mysql/data   # Remote directory for the data files

[ndbd]
hostname=gmoles4    # Hostname of the second data node
datadir=/usr/local/mysql/data   # Remote directory for the data files

[mysqld]
# SQL node options:
hostname=gmoles1   # In our case the MySQL server/client is on the same Droplet as the cluster manager
" >> /var/lib/mysql-cluster/config.ini

echo "[mysqld]
ndbcluster # run NDB storage engine" >> /etc/my.cnf
sudo groupadd mysql
sudo useradd -r -g mysql -s /bin/false mysql
sudo /opt/mysql/server-5.7/bin/mysql_secure_installation
sudo cp /opt/mysql/server-5.7/support-files/mysql.server /etc/init.d/mysqld
sudo systemctl start mysqld
sudo ln -s /opt/mysql/server-5.7/bin/mysql /usr/bin/

