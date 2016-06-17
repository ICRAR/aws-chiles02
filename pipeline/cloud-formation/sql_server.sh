#!/usr/bin/env bash

sudo yum update -y
sudo yum install mysql-server xfsprogs -y

sudo /etc/init.d/mysqld start
mysqladmin -u root password 'XXXXX'

## MySQL bits
CREATE USER 'root'@'%' IDENTIFIED BY 'XXXXXX';
GRANT ALL ON *.* TO 'root'@'%';
flush privileges;
CREATE SCHEMA `chiles02` ;
