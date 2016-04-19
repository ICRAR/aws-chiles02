#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
#
#   .oooooo.   oooo         o8o  oooo                       .oooo.     .oooo.
#  d8P'  `Y8b  `888         `"'  `888                      d8P'`Y8b  .dP""Y88b
# 888           888 .oo.   oooo   888   .ooooo.   .oooo.o 888    888       ]8P'
# 888           888P"Y88b  `888   888  d88' `88b d88(  "8 888    888     .d8P'
# 888           888   888   888   888  888ooo888 `"Y88b.  888    888   .dP'
# `88b    ooo   888   888   888   888  888    .o o.  )88b `88b  d88' .oP     .o
#  `Y8bood8P'  o888o o888o o888o o888o `Y8bod8P' 8""888P'  `Y8bd8P'  8888888888
#
#
# Create a container for the Chiles02 code

FROM casapy:latest

RUN yum update -y

RUN yum install -y \
    git \
    vim; \
    yum clean all

RUN mkdir -p /opt/chiles02
RUN chmod oug+rw /opt/chiles02

# Copy the scripts
COPY scripts /opt/chiles02/scripts/

# Make them executable
RUN chmod oug+x /opt/chiles02/scripts/*.sh

# Update the path
env PATH /opt/chiles02/scripts:$PATH
env PYTHONPATH $PYTHONPATH:/opt/chiles02/aws-chiles02/pipeline

VOLUME /dfms_root

# Add the ec2-user
RUN groupadd -g500 ec2-user
RUN useradd -ms /bin/bash -u500 -g500 ec2-user

# Clone the image as ec2-user as we run things as ec2-user
USER ec2-user
WORKDIR /opt/chiles02
RUN git clone https://github.com/ICRAR/aws-chiles02.git
