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

package com.github.perftool.mq.producer.mqtt;

import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricBean;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.module.OperationType;
import com.github.perftool.mq.producer.common.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
public class MqttSendThread extends AbstractProduceThread {

    private final MqttConfig mqttConfig;

    private final List<MqttClient> mqttClients;

    private final Random random;

    private final MetricBean metricBean;

    public MqttSendThread(int index, MetricFactory metricFactory, ThreadConfig config, MqttConfig mqttConfig) {
        super(index, metricFactory, config);
        this.mqttConfig = mqttConfig;
        this.mqttClients = new ArrayList<>();
        this.random = new Random();
        this.metricBean = newMetricBean(OperationType.PRODUCE);
    }

    @Override
    public void init() throws Exception {
        for (int i = 0; i < mqttConfig.producerNum; i++) {
            MqttClient mqttClient = new MqttClient(String.format("tcp://%s:%d", mqttConfig.host, mqttConfig.port),
                    mqttConfig.clientId);
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setUserName(mqttConfig.username);
            mqttConnectOptions.setPassword(mqttConfig.password.toCharArray());
            mqttClient.connect(mqttConnectOptions);
            mqttClients.add(mqttClient);
        }
    }

    @Override
    protected void send() {
        long startTime = System.currentTimeMillis();
        try {
            MqttMessage mqttMessage = getMqttMessage(mqttConfig.messageByte);
            mqttClients.get(random.nextInt(mqttConfig.producerNum)).publish(mqttConfig.topic,
                    mqttMessage);
        } catch (Exception e) {
            metricBean.fail(System.currentTimeMillis() - startTime);
            log.error("send req exception ", e);
        }
    }

    private MqttMessage getMqttMessage(int messageByte) {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setQos(0);
        mqttMessage.setPayload(RandomUtil.getRandomBytes(messageByte));
        return mqttMessage;
    }

}
