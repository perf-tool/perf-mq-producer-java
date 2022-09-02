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

package com.github.perftool.mq.producer.rocketmq;

import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricBean;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
public class RocketMqThread extends AbstractProduceThread {

    private final RocketMqConfig config;

    private final Random random;

    private final List<DefaultMQProducer> producers;

    private final MetricBean metricBean;

    public RocketMqThread(int index, MetricFactory metricFactory, ThreadConfig config, RocketMqConfig rocketMqConfig) {
        super(index, metricFactory, config);
        this.config = rocketMqConfig;
        this.producers = new ArrayList<>();
        this.random = new Random();
        this.metricBean = newMetricBean();

    }

    @Override
    public void init() throws Exception {
        for (int i = 0; i < config.producerNum; i++) {
            DefaultMQProducer producer = new
                    DefaultMQProducer(config.groupName + "_" + i);
            // Specify name server addresses.
            producer.setNamesrvAddr(config.rocketMqAddr);
            //Launch the instance.
            producer.start();
            producers.add(producer);
        }
    }

    @Override
    protected void send() {

        long startTime = System.currentTimeMillis();
        int index = random.nextInt(config.producerNum);
        String topic = config.topic + "_" + index;
        DefaultMQProducer producer = producers.get(index);
        try {
            Message msg = new Message(topic, RandomUtil.getRandomBytes(config.messageByte));
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    metricBean.success(System.currentTimeMillis() - startTime);
                    log.info("msg send to {} success. msgId {} queueOffset {}",
                            topic, sendResult.getMsgId(), sendResult.getQueueOffset());
                }

                @Override
                public void onException(Throwable e) {
                    metricBean.fail(System.currentTimeMillis() - startTime);
                    log.error("{} send msg callback failed", topic, e);
                }
            });
        } catch (MQClientException | RemotingException | InterruptedException e) {
            log.error("send msg to {} failed", topic, e);
        }
    }
}
