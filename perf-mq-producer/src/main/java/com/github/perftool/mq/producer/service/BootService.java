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

package com.github.perftool.mq.producer.service;

import com.github.perftool.mq.producer.config.PfConfig;
import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.CommonConfig;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.service.MetricsService;
import com.github.perftool.mq.producer.http.HttpConfig;
import com.github.perftool.mq.producer.http.HttpSendService;
import com.github.perftool.mq.producer.kafka.KafkaConfig;
import com.github.perftool.mq.producer.kafka.KafkaSendService;
import com.github.perftool.mq.producer.common.module.ProduceType;
import com.github.perftool.mq.producer.mqtt.MqttConfig;
import com.github.perftool.mq.producer.mqtt.MqttSendService;
import com.github.perftool.mq.producer.pulsar.PulsarConfig;
import com.github.perftool.mq.producer.pulsar.PulsarSendService;
import com.github.perftool.mq.producer.rocketmq.RocketMqConfig;
import com.github.perftool.mq.producer.rocketmq.RocketMqService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class BootService {

    @Autowired
    private CommonConfig commonConfig;

    @Autowired
    private ThreadConfig threadConfig;

    @Autowired
    private PfConfig pfConfig;

    @Autowired
    private HttpConfig httpConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private MqttConfig mqttConfig;

    @Autowired
    private PulsarConfig pulsarConfig;

    @Autowired
    private RocketMqConfig rocketMqConfig;

    @Autowired
    private MetricsService metricsService;

    private final List<AbstractProduceThread> threads = new ArrayList<>();

    @PostConstruct
    public void init() throws Exception {
        final MetricFactory metricFactory = metricsService.acquireMetricFactory(pfConfig.produceType);
        for (int i = 0; i < commonConfig.workNum; i++) {
            if (pfConfig.produceType.equals(ProduceType.HTTP)) {
                threads.add(new HttpSendService(i, metricFactory, threadConfig, httpConfig));
            } else if (pfConfig.produceType.equals(ProduceType.KAFKA)) {
                threads.add(new KafkaSendService(i, metricFactory, threadConfig, kafkaConfig));
            } else if (pfConfig.produceType.equals(ProduceType.MQTT)) {
                threads.add(new MqttSendService(i, metricFactory, threadConfig, mqttConfig));
            } else if (pfConfig.produceType.equals(ProduceType.PULSAR)) {
                threads.add(new PulsarSendService(i, metricFactory, threadConfig, pulsarConfig));
            } else if (pfConfig.produceType.equals(ProduceType.ROCKETMQ)) {
                threads.add(new RocketMqService(i, metricFactory, threadConfig, rocketMqConfig));
            }
        }
        for (AbstractProduceThread thread : threads) {
            thread.init();
            thread.start();
        }
    }

}
