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
#   .oooooo.                                ooooooooo.
#  d8P'  `Y8b                               `888   `Y88.
# 888           .oooo.    .oooo.o  .oooo.    888   .d88' oooo    ooo
# 888          `P  )88b  d88(  "8 `P  )88b   888ooo88P'   `88.  .8'
# 888           .oP"888  `"Y88b.   .oP"888   888           `88..8'
# `88b    ooo  d8(  888  o.  )88b d8(  888   888            `888'
#  `Y8bood8P'  `Y888""8o 8""888P' `Y888""8o o888o            .8'
#                                                        .o..P'
#                                                        `Y8P'
#
# Create a container to run the latest version of CasaPy

FROM centos:latest

RUN yum update -y
RUN yum install -y \
    fontconfig \
    freetype \
    libpng12 \
    libSM \
    libXcursor \
    libXi \
    libXinerama \
    libXrandr \
    libXfixes \
    libXrender \
    libxslt \
    which \
    xauth \
    xorg-x11-server-Xvfb

RUN mkdir -p /opt/casapy
RUN curl -SL https://svn.cv.nrao.edu/casa/linux_distro/release/el6/casa-release-4.5.0-el6.tar.gz \
    | tar -xzC /opt/casapy

RUN ulimit -n 2048
RUN curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | python

# Update the path
ENV PATH $PATH:/opt/casapy/casa-release-4.5.0-el6/bin

# Make sure CasaPy is initialised
RUN echo exit | casapy --nologger --log2term
