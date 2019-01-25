#!/bin/bash

cat ms.txt | xargs -I {} -P 5 -n 1 tar -xzf {}