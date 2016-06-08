#!/usr/bin/env bash

sudo yum update -y
sudo yum install mysql-server xfsprogs -y
sudo mkdir -p /mnt/daliuge
sudo mkfs.xfs -K /dev/xvdb

sudo mkdir -p /mnt/daliuge/mysql
sudo vim /etc/my.cnf
# Point to directory

sudo /etc/init.d/mysqld start
mysqladmin -u root password 'XXXXX'

