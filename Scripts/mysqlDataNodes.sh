#!/bin/bash
sed -i 's/127.0.0.1 localhost/127.0.0.1 localhost manager\n172.16.4.15 gmoles1\n172.16.4.16 gmoles2\n172.16.4.18 gmoles3\n172.16.4.21 gmoles4/g' /etc/hosts
sudo apt-get install libaio1
wget https://dev.mysql.com/get/Downloads/MySQL-Cluster-7.5/mysql-cluster-gpl-7.5.5-debian8-x86_64.deb
sudo dpkg -i mysql-cluster-gpl-7.5.5-debian8-x86_64.deb
echo "[mysql_cluster]
ndb-connectstring=gmoles1" >> /etc/my.cnf
sudo mkdir -p /usr/local/mysql/data
