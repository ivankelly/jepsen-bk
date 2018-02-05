#!/usr/bin/env bash
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

CONF_FILE=/etc/zookeeper/conf/zoo.cfg
ID_FILE=/var/lib/zookeeper/myid

IDX=1
for SERVER in $(echo $zkServers | tr "," "\n")
do
    echo "server.$IDX=$SERVER:2888:3888" >> $CONF_FILE

    if [ "$HOSTNAME" == "$SERVER" ]; then
	echo "Current server id $IDX"
        echo $IDX > $ID_FILE
    fi

    ((IDX++))
done
