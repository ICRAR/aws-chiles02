#!/usr/bin/env bash

sudo yum update -y
sudo yum install mysql-server -y

sudo setenforce 0

sudo /etc/init.d/mysqld start
sudo chkconfig mysqld on
mysqladmin -u root password 'XXXXX'

## MySQL bits
CREATE USER 'root'@'%' IDENTIFIED BY 'XXXXXX';
GRANT ALL ON *.* TO 'root'@'%';
flush privileges;
CREATE SCHEMA `chiles02` ;
