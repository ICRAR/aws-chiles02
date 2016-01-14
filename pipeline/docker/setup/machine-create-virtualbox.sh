#!/usr/bin/env bash
# As casapy is quite large we need to have a reasonable size
docker-machine create -d virtualbox --virtualbox-memory 2048 --virtualbox-disk-size 20480 casapy
docker-machine env casapy
docker build --tag casapy:latest .
