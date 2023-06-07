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

package com.github.perftool.mq.producer.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class KafkaConfig {

    @Value("${KAFKA_ADDR:localhost:9092}")
    public String kafkaAddr;

    @Value("${KAFKA_PRODUCER_NUM_PER_THREAD:1}")
    public int producerNum;

    @Value("${KAFKA_TOPIC:topic}")
    public String topic;

    @Value("${KAFKA_MESSAGE_BYTE:1024}")
    public int messageByte;

    @Value("${KAFKA_IDEMPOTENCE:false}")
    public boolean idempotence;

    @Value("${KAFKA_ACKS:1}")
    public String acks;

    /**
     * default value is 16kb
     */
    @Value("${KAFKA_BATCH_SIZE_KB:16}")
    public int batchSizeKb;

    @Value("${KAFKA_LINGER_MS:0}")
    public int lingerMS;

    /**
     * none, gzip, lz4, snappy, zstd
     */
    @Value("${KAFKA_COMPRESSION_TYPE:none}")
    public String compressionType;

    @Value("${KAFKA_SASL_ENABLE:false}")
    public boolean saslEnable;

    @Value("${KAFKA_SASL_MECHANISM:PLAIN}")
    public String saslMechanism;

    @Value("${KAFKA_SASL_USERNAME:}")
    public String saslUsername;

    @Value("${KAFKA_SASL_PASSWORD:}")
    public String saslPassword;

    @Value("${KAFKA_SASL_SSL_ENABLE:false}")
    public boolean saslSslEnable;

    @Value("${KAFKA_SASL_SSL_TRUSTSTORE_LOCATION:}")
    public String saslSslTrustStoreLocation;

    @Value("${KAFKA_SASL_SSL_TRUSTSTORE_PASSWORD:}")
    public String saslSslTrustStorePassword;

}
