#!/bin/bash
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

cd "$(dirname "$0")"

cd ..

PF_HOME=`pwd`

echo "${PF_HOME}"

mkdir -p /opt/perf/logs

if [ -n "${PULSAR_JAR_VERSION}" ] && [ -n "${MAVEN_ADDRESS}" ]; then
  # delete original version jar of pulsar
  rm -rf "${PF_HOME}"/lib/pulsar-client*
  rm -rf "${PF_HOME}"/lib/pulsar-common*
  rm -rf "${PF_HOME}"/lib/pulsar-package-core*
  rm -rf "${PF_HOME}"/lib/pulsar-transaction-common*

  # download specify version jar of pulsar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-client-admin-api/"${PULSAR_JAR_VERSION}"/pulsar-client-admin-api-"${PULSAR_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-client-admin-original/"${PULSAR_JAR_VERSION}"/pulsar-client-admin-original-"${PULSAR_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-client-api/"${PULSAR_JAR_VERSION}"/pulsar-client-api-"${PULSAR_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-client-original/"${PULSAR_JAR_VERSION}"/pulsar-client-original-"${PULSAR_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-common/"${PULSAR_JAR_VERSION}"/pulsar-common-"${PULSAR_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-package-core/"${PULSAR_JAR_VERSION}"/pulsar-package-core-"${PULSAR_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/pulsar/pulsar-transaction-common/"${PULSAR_JAR_VERSION}"/pulsar-transaction-common-"${PULSAR_JAR_VERSION}".jar
fi

if [ -n "${BOOKKEEPER_JAR_VERSION}" ] && [ -n "${MAVEN_ADDRESS}" ]; then
  # delete original version jar of bookkeeper
  rm -rf "${PF_HOME}"/lib/bookkeeper-common-allocator*
  # download specify version jar of bookkeeper
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/bookkeeper/bookkeeper-common-allocator/"${BOOKKEEPER_JAR_VERSION}"/bookkeeper-common-allocator-"${BOOKKEEPER_JAR_VERSION}".jar
fi

if [ -n "${APOLLO_JAR_VERSION}" ] && [ -n "${MAVEN_ADDRESS}" ]; then
  rm -rf "${PF_HOME}"/lib/apollo*

  # download specify version jar of apollo
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/com/ctrip/framework/apollo/apollo-client/"${APOLLO_JAR_VERSION}"/apollo-client-"${APOLLO_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/com/ctrip/framework/apollo/apollo-core/"${APOLLO_JAR_VERSION}"/apollo-core-"${APOLLO_JAR_VERSION}".jar
  wget -P "${PF_HOME}"/lib  --no-check-certificate "${MAVEN_ADDRESS}"/com/ctrip/framework/apollo/apollo-openapi/"${APOLLO_JAR_VERSION}"/apollo-openapi-"${APOLLO_JAR_VERSION}".jar
fi

if [ -n "${KAFKA_JAR_VERSION}" ] && [ -n "${MAVEN_ADDRESS}" ]; then
  rm -rf "${PF_HOME}"/lib/kafka-client*

  # download specify version jar of kafka
  # such as: https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
  wget -P "${PF_HOME}"/lib --no-check-certificate "${MAVEN_ADDRESS}"/org/apache/kafka/kafka-clients/"${KAFKA_JAR_VERSION}"/kafka-clients-"${KAFKA_JAR_VERSION}".jar
fi

# memory option
if [ ! -n "$HEAP_MEM" ]; then
  HEAP_MEM="1G"
fi
if [ ! -n "$DIR_MEM" ]; then
  DIR_MEM="1G"
fi
# mem option
JVM_OPT="-Xmx${HEAP_MEM} -Xms${HEAP_MEM} -XX:MaxDirectMemorySize=${DIR_MEM}"
# gc option
if [ ! -n "${GC_THREADS}" ]; then
  GC_THREADS=1
fi
JVM_OPT="${JVM_OPT} -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions"
JVM_OPT="${JVM_OPT} -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=${GC_THREADS} -XX:ConcGCThreads=${GC_THREADS}"
# gc log option
JVM_OPT="${JVM_OPT} -Xlog:gc*=info,gc+phases=debug:/opt/perf/logs/gc.log:time,uptime:filecount=10,filesize=100M"

# skywalking java agent option
if [ -n "${SW_AGENT_ENABLE}" ]; then
  if [ ! -n "${SW_SERVICE_NAME}" ]; then
    SW_SERVICE_NAME="perf-mq-producer"
  fi
  if [ ! -n "${SW_COLLECTOR_URL}" ]; then
    SW_COLLECTOR_URL="localhost:11800"
  fi
  # ignore springmvc agent plugin
  rm -rf /opt/perf/skywalking-agent/plugins/apm-springmvc-annotation*
  AGENT_OPT="-javaagent:/opt/perf/skywalking-agent/skywalking-agent.jar -Dskywalking.agent.service_name=${SW_SERVICE_NAME} -Dskywalking.collector.backend_service=${SW_COLLECTOR_URL}"
fi

java $AGENT_OPT $JAVA_OPT $JVM_OPT -Dlog4j.configurationFile=conf/log4j2.xml -classpath /opt/perf/lib/*:/opt/perf/pf-producer.jar:/opt/perf/conf/* com.github.perftool.mq.producer.Main >>/opt/perf/logs/stdout.log 2>>/opt/perf/logs/stderr.log
