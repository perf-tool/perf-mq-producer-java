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

import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricBean;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.module.OperationType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class KafkaSendService extends AbstractProduceThread {

    private final KafkaConfig kafkaConfig;

    private final List<KafkaProducer<String, String>> producers;

    private final Random random;

    private final MetricBean metricBean;

    public KafkaSendService(int index, MetricFactory metricFactory, ThreadConfig threadConfig,
                            KafkaConfig kafkaConfig) {
        super(index, metricFactory, threadConfig);
        this.kafkaConfig = kafkaConfig;
        this.producers = new ArrayList<>();
        this.random = new Random();
        this.metricBean = newMetricBean(OperationType.PRODUCE);
    }

    @Override
    public void init() {
        for (int i = 0; i < kafkaConfig.producerNum; i++) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.kafkaAddr);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaConfig.idempotence);
            props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.acks);
            props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.lingerMS);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaConfig.compressionType);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(kafkaConfig.batchSizeKb * 1024));
            if (kafkaConfig.saslEnable) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
                props.put(SaslConfigs.SASL_MECHANISM, kafkaConfig.saslMechanism);
                String saslJaasConfig = String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required %n"
                                + "username=\"%s\" %npassword=\"%s\";",
                        kafkaConfig.saslUsername, kafkaConfig.saslPassword);
                props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
            }
            producers.add(new KafkaProducer<>(props));
        }
    }

    @Override
    protected void send() {
        long startTime = System.currentTimeMillis();
        try {
            ProducerRecord<String, String> record = getRecord(kafkaConfig.topic, kafkaConfig.messageByte);
            producers.get(random.nextInt(kafkaConfig.producerNum)).send(record, (recordMetadata, e) -> {
                if (e != null) {
                    metricBean.fail(System.currentTimeMillis() - startTime);
                    log.error("exception is ", e);
                } else {
                    metricBean.success(System.currentTimeMillis() - startTime);
                    log.debug("send record to [{}]", record.topic());
                }
            });
        } catch (Exception e) {
            metricBean.fail(System.currentTimeMillis() - startTime);
            log.error("send req exception ", e);
        }
    }

    private ProducerRecord<String, String> getRecord(String topic, int messageByte) {
        StringBuilder messageBuilder = new StringBuilder(messageByte);
        for (int i = 0; i < messageByte; i++) {
            messageBuilder.append('a' + random.nextInt(26));
        }
        return new ProducerRecord<>(topic, "key", messageBuilder.toString());
    }


}
