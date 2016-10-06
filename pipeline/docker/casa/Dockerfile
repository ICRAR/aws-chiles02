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
#   .oooooo.
#  d8P'  `Y8b
# 888           .oooo.    .oooo.o  .oooo.
# 888          `P  )88b  d88(  "8 `P  )88b
# 888           .oP"888  `"Y88b.   .oP"888
# `88b    ooo  d8(  888  o.  )88b d8(  888
#  `Y8bood8P'  `Y888""8o 8""888P' `Y888""8o
#
#
# Create a container to run the latest version of Casa

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
    perl \
    which \
    xauth \
    xorg-x11-server-Xvfb; \
    yum clean all

RUN mkdir -p /opt/casa
RUN curl -SL https://svn.cv.nrao.edu/casa/linux_distro/release/el6/casa-release-4.7.0-el6.tar.gz \
    | tar -xzC /opt/casa

RUN curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | python

# Update the path
ENV PATH $PATH:/opt/casa/casa-release-4.7.0-el6/bin

# Make sure Casa is initialised
RUN echo exit | casa --nologger --log2term
