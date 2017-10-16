#!/usr/bin/env bash
# As casa is quite large we need to have a reasonable size
docker-machine create -d virtualbox --virtualbox-memory 2048 --virtualbox-disk-size 20480 casa
docker-machine env casa
docker build --tag casa:latest .
