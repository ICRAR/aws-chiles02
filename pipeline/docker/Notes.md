The figlet code
===============

The code use to build the figlets is [here] (http://patorjk.com/software/taag/#p=display&f=Roman&t=Chiles02)

Downloading MS
==============
To start the docker container to download a MS

`docker run -i -t -v /mnt/dfms/dfms_root:/dfms_root sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest copy_from_s3_mt.sh s3://13b-266/13B-266.sb28624226.eb28625769.56669.43262586805_calibrated_deepfield.ms.tar /dfms_root/ms_test1 <aws key> <aws secret key>`


MS Transform Testing
====================

Start the Chiles02 docker container. 
As the container is over 2.5GB start the container and use git to update the code
  
`docker run -i -t -v /mnt/dfms/dfms_root:/dfms_root sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest /bin/bash`  

`mstransform.sh /dfms_root/123456/ /dfms_root/output_test/ /dfms_root/log_dir/log.log 980 984`
