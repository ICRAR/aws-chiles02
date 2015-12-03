Where the BASH code lives
-------------------------

In principle, we should put most of the code inside Python / boto, and less code in shell scripts

Added cubejob-pbs-hack.sh which works on AMI playpen to run the pleaides PBS scripts to make the cubes.

start_*.sh
----------

These scripts are used to start casapy

run_*.sh
--------

These scripts are used to build the user_data that is passed to the AWS instances when they start up.
