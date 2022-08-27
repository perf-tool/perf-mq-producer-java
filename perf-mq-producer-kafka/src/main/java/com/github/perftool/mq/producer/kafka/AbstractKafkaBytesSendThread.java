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

import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.util.RandomUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class AbstractKafkaBytesSendThread extends AbstractKafkaSendThread<byte[]> {

    public AbstractKafkaBytesSendThread(int index, MetricFactory metricFactory,
                                        ThreadConfig threadConfig, KafkaConfig kafkaConfig) {
        super(index, metricFactory, threadConfig, kafkaConfig);
    }

    @Override
    protected String getValueSerializerName() {
        return ByteArraySerializer.class.getName();
    }

    @Override
    protected ProducerRecord<byte[], byte[]> getRecord(String topic, int messageByte) {
        return new ProducerRecord<>(topic, RandomUtil.getRandomBytes(messageByte));
    }
}
