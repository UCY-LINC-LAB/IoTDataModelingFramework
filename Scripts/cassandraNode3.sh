#!/bin/bash
echo "deb http://www.apache.org/dist/cassandra/debian 39x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
curl -k https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA
sudo apt-get update
sudo apt-get install cassandra
sudo service cassandra stop
sudo rm -rf /var/lib/cassandra/data/system/*
sed -i 's/seeds: "127.0.0.1"/seeds: "gmoles1,gmoles2,gmoles3,gmoles4"/g' /etc/cassandra/cassandra.yaml
sed -i 's/listen_address: localhost/listen_address: gmoles3/g' /etc/cassandra/cassandra.yaml
sed -i 's/rpc_address: localhost/rpc_address: 0.0.0.0/g' /etc/cassandra/cassandra.yaml
sed -i 's/endpoint_snitch: SimpleSnitch/endpoint_snitch: GossipingPropertyFileSnitch/g' /etc/cassandra/cassandra.yaml
echo "auto_bootstrap: false" >> /etc/cassandra/cassandra.yaml




sed -i 's/old-word/new-word/g' *.txt


