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

import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricBean;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.module.OperationType;
import com.github.perftool.mq.producer.common.util.NameUtil;
import com.github.perftool.mq.producer.common.util.RandomUtil;
import com.github.perftool.mq.producer.pulsar.util.PulsarUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarSendService extends AbstractProduceThread {

    private final PulsarConfig pulsarConfig;

    private final List<Producer<byte[]>> producers;

    private final Random random;

    private final MetricBean metricBean;

    public PulsarSendService(int index, MetricFactory metricFactory, ThreadConfig threadConfig,
                             PulsarConfig pulsarConfig) {
        super(index, metricFactory, threadConfig);
        this.producers = new ArrayList<>();
        this.pulsarConfig = pulsarConfig;
        this.random = new Random();
        this.metricBean = newMetricBean(OperationType.PRODUCE);
    }

    @Override
    public void init() throws Exception {
        PulsarClient client = PulsarClient.builder().memoryLimit(pulsarConfig.memoryLimitMb, SizeUnit.MEGA_BYTES)
                .serviceUrl(String.format("http://%s:%s", pulsarConfig.host, pulsarConfig.port)).build();
        List<String> topics = getTopicList();
        for (int i = 0; i < pulsarConfig.producerNum; i++) {
            for (String topic : topics){
                Producer<byte[]> producer = client.newProducer()
                        .maxPendingMessages(pulsarConfig.maxPendingMessage)
                        .enableBatching(pulsarConfig.enableBatching)
                        .batchingMaxBytes(pulsarConfig.batchingMaxBytes)
                        .batchingMaxMessages(pulsarConfig.batchingMaxMessages)
                        .batchingMaxPublishDelay(pulsarConfig.batchingMaxPublishDelay, TimeUnit.MILLISECONDS)
                        .topic(topic).create();
                producers.add(producer);
            }
        }
    }

    @Override
    protected void send() {
        long startTime = System.currentTimeMillis();
        try {
            CompletableFuture<MessageId> messageIdCompletableFuture = producers
                    .get(random.nextInt(pulsarConfig.producerNum))
                    .sendAsync(RandomUtil.getRandomBytes(pulsarConfig.messageByte));
            messageIdCompletableFuture.whenComplete((messageId, throwable) -> {
                if (throwable != null) {
                    metricBean.fail(System.currentTimeMillis() - startTime);
                    log.error("error is ", throwable);
                } else {
                    metricBean.success(System.currentTimeMillis() - startTime);
                    log.info("message id is [{}]", messageId);
                }
            });
        } catch (Exception e) {
            metricBean.fail(System.currentTimeMillis() - startTime);
            log.error("send req exception ", e);
        }
    }

    private List<String> getTopicList() {
        List<String> topics = new ArrayList<>();
        log.info("tenant prefix name [{}].", pulsarConfig.tenantPrefix);
        if (!pulsarConfig.tenantPrefix.isBlank()) {
            if (pulsarConfig.namespacePrefix.isBlank()) {
                log.info("namespace prefix name is blank.");
                return topics;
            }
            List<String> namespaces = namespaces();
            if (pulsarConfig.tenantSuffixNum == 0) {
                String tenantName = pulsarConfig.tenantPrefix;
                topics = topics(tenantName, namespaces);
            } else {
                for (int i = 0; i < pulsarConfig.tenantSuffixNum; i++) {
                    String tenantName = NameUtil.name(pulsarConfig.tenantPrefix,
                            i, pulsarConfig.tenantSuffixNumOfDigits);
                    topics.addAll(topics(tenantName, namespaces));
                }
            }
        } else {
            if (pulsarConfig.topicSuffixNum == 0) {
                topics.add(PulsarUtils.topicFn(pulsarConfig.tenant, pulsarConfig.namespace, pulsarConfig.topic));
            } else {
                for (int i = 0; i < pulsarConfig.topicSuffixNum; i++) {
                    topics.add(PulsarUtils.topicFn(pulsarConfig.tenant, pulsarConfig.namespace,
                            pulsarConfig.topic + i));
                }
            }
        }
        return topics;
    }

    private List<String> namespaces() {
        List<String> namespaceNames = new ArrayList<>();
        if (pulsarConfig.namespaceSuffixNum == 0) {
            namespaceNames.add(pulsarConfig.namespacePrefix);
        }
        for (int i = 0; i < pulsarConfig.namespaceSuffixNum; i++) {
            String namespaceName = NameUtil.name(pulsarConfig.namespacePrefix, i
                    , pulsarConfig.namespaceSuffixNumOfDigits);
            namespaceNames.add(namespaceName);
        }
        return namespaceNames;
    }

    private List<String> topics(String tenantName, List<String> namespaceNames) {
        List<String> topics = new ArrayList<>();
        if (pulsarConfig.topicSuffixNum == 0) {
            for (String namespaceName : namespaceNames) {
                topics.add(PulsarUtils.topicFn(tenantName, namespaceName, pulsarConfig.topic));
            }
        } else {
            for (int i = 0; i < pulsarConfig.topicSuffixNum; i++) {
                String topicName = NameUtil.name(pulsarConfig.topic, i, pulsarConfig.topicSuffixNumOfDigits);
                for (String namespaceName : namespaceNames) {
                    topics.add(PulsarUtils.topicFn(tenantName, namespaceName, topicName));
                }
            }
        }
        return topics;
    }
}
