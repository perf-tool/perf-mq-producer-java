/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.mq.producer.pulsar;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class PulsarConfig {

    @Value("${PULSAR_PROTOCOL:http}")
    public String protocol;

    @Value("${PULSAR_HOST:localhost}")
    public String host;

    @Value("${PULSAR_PORT:8080}")
    public int port;

    @Value("${PULSAR_IO_THREADS:4}")
    public int pulsarIoThreads;

    @Value("${PULSAR_MAX_CONCURRENT_LOOKUP_REQUESTS:5000}")
    public int pulsarMaxConcurrentLookupRequests;

    @Value("${PULSAR_TENANT:public}")
    public String tenant;

    @Value("${PULSAR_TENANT_PREFIX:}")
    public String tenantPrefix;

    @Value("${PULSAR_TENANT_SUFFIX_NUM:0}")
    public int tenantSuffixNum;

    @Value("${PULSAR_TENANT_SUFFIX_NUM_OF_DIGITS:0}")
    public int tenantSuffixNumOfDigits;

    @Value("${PULSAR_NAMESPACE:default}")
    public String namespace;

    @Value("${PULSAR_NAMESPACE_PREFIX:}")
    public String namespacePrefix;

    @Value("${PULSAR_NAMESPACE_SUFFIX_NUM:0}")
    public int namespaceSuffixNum;

    @Value("${PULSAR_NAMESPACE_SUFFIX_NUM_OF_DIGITS:0}")
    public int namespaceSuffixNumOfDigits;

    @Value("${PULSAR_TOPIC_SUFFIX_NUM:0}")
    public int topicSuffixNum;

    @Value("${PULSAR_TOPIC_SUFFIX_NUM_OF_DIGITS:0}")
    public int topicSuffixNumOfDigits;

    @Value("${PULSAR_PRODUCER_NUM_PER_TOPIC_PER_THREAD:1}")
    public int producerNum;

    @Value("${PULSAR_TOPIC:topic}")
    public String topic;

    @Value("${PULSAR_MESSAGE_BYTE:1024}")
    public int messageByte;

    @Value("${PULSAR_MEMORY_LIMIT_MB:50}")
    public int memoryLimitMb;

    @Value("${PULSAR_MAX_PENDING_MESSAGE:1000}")
    public int maxPendingMessage;

    @Value("${PULSAR_ENABLE_BATCHING:true}")
    public boolean enableBatching;

    @Value("${PULSAR_BATCHING_MAX_BYTES:131072}")
    public int batchingMaxBytes;

    @Value("${PULSAR_BATCHING_MAX_MESSAGES:1000}")
    public int batchingMaxMessages;

    @Value("${PULSAR_BATCHING_MAX_PUBLISH_DELAY_MS:1}")
    public long batchingMaxPublishDelay;

    @Value("${PULSAR_TLS_ENABLE:false}")
    public boolean tlsEnable;

    @Value("${PULSAR_KEYSTORE_PATH:}")
    public String keyStorePath;

    @Value("${PULSAR_KEYSTORE_PASSWORD:}")
    public String keyStorePassword;

    @Value("${PULSAR_TLS_TRUSTSTORE_PATH:}")
    public String tlsTrustStorePath;

    @Value("${PULSAR_TLS_TRUSTSTORE_PASSWORD:}")
    public String tlsTrustStorePassword;

    @Value("${PULSAR_AUTH_TOKEN_ENABLE:false}")
    public boolean authTokenEnable;

    @Value("${PULSAR_AUTH_TOKEN:}")
    public String authToken;

    @Value("${PULSAR_SECURE_CONNECTION_ENABLE:false}")
    public boolean secureConnectionEnable;

    @Value("${TRACE_REPORT_SCENE:default}")
    public String traceReportScene;

    @Value("${COMPRESSION_TYPE:NONE}")
    public String compressionType;

}
