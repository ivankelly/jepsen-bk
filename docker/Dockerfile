#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
FROM openjdk:8-jdk

ARG BK_TARBALL

ENV zkServers MUST_CONFIGURE

# Install some utilities
RUN apt-get update && apt-get install -qy wget curl emacs-nox man-db faketime \
    ntpdate unzip iptables psmisc tar bzip2 openjdk-8-jdk iputils-ping \
    iproute rsyslog logrotate zookeeperd zookeeper zookeeper-bin supervisor \
    openssh-server netcat-openbsd sudo

ADD target/register-service /opt/register-service

ADD ${BK_TARBALL} /opt
RUN mv /opt/bookkeeper* /opt/bookkeeper \
    && mkdir /var/log/bookkeeper \
    && mkdir /var/log/register-service \
    && mkdir /var/run/supervisor

# configure ssh, disable auth for root
RUN mkdir -p /run/sshd \
    && sed -i 's/#\?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/#\?PermitEmptyPasswords.*/PermitEmptyPasswords yes/' /etc/ssh/sshd_config \
    && sed -i 's/#\?UsePAM.*/UsePAM no/' /etc/ssh/sshd_config \	
    && echo root:root | chpasswd

RUN sed -i 's/ZOO_LOG4J_PROP=.*/ZOO_LOG4J_PROP=INFO,CONSOLE/' /etc/zookeeper/conf/environment
ADD conf/supervisord.conf /etc/supervisord.conf
ADD conf/supervisor-zookeeper.conf /etc/supervisord/conf.d/zookeeper.conf
ADD conf/supervisor-bookkeeper.conf /etc/supervisord/conf.d/bookkeeper.conf
ADD conf/supervisor-register-service.conf /etc/supervisord/conf.d/register-service.conf
ADD conf/supervisor-ssh.conf /etc/supervisord/conf.d/ssh.conf

ADD scripts/*.sh /
CMD /configure-and-run.sh