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

package com.github.perftool.mq.producer.bookkeeper;

import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricBean;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
public class BookkeeperSendThread extends AbstractProduceThread {

    private final BookkeeperConfig bookkeeperConfig;

    private final List<RotateLedger> rotateLedgerList;

    private final MetricBean metricBean;

    private final Random random;

    private BookKeeper bookKeeper;

    public BookkeeperSendThread(int index, MetricFactory metricFactory, ThreadConfig config,
                                BookkeeperConfig bookkeeperConfig) {
        super(index, metricFactory, config);
        this.rotateLedgerList = new ArrayList<>();
        this.bookkeeperConfig = bookkeeperConfig;
        this.metricBean = newMetricBean();
        this.random = new Random();
    }

    @Override
    public void init() throws Exception {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMetadataServiceUri("zk+null://" + bookkeeperConfig.servers + "/ledgers");
        clientConfiguration.setNumIOThreads(bookkeeperConfig.ioThreads);
        clientConfiguration.setNumWorkerThreads(bookkeeperConfig.workerThreads);
        clientConfiguration.setAddEntryTimeout(bookkeeperConfig.addEntryTimeoutSeconds);
        clientConfiguration.setReadEntryTimeout(bookkeeperConfig.readEntryTimeoutSeconds);
        clientConfiguration.setNumChannelsPerBookie(bookkeeperConfig.channelsPerBookie);
        clientConfiguration.setUseV2WireProtocol(bookkeeperConfig.useV2WireProtocol);
        this.bookKeeper = new BookKeeper(clientConfiguration);
        for (int i = 0; i < bookkeeperConfig.ledgerNum; i++) {
            RotateLedger rotateLedger = new RotateLedger(bookKeeper, bookkeeperConfig);
            rotateLedgerList.add(rotateLedger);
        }
    }

    @Override
    protected void send() {
        RotateLedger rotateLedger = rotateLedgerList.get(random.nextInt(rotateLedgerList.size()));
        long startTime = System.currentTimeMillis();
        try {
            rotateLedger.send(RandomUtil.getRandomBytes(bookkeeperConfig.messageByte), (rc, lh, entryId, ctx) -> {
                if (rc != BKException.Code.OK) {
                    metricBean.fail(System.currentTimeMillis() - startTime);
                    log.error("ledger {} entry {} add entry failed: {}",
                            lh.getId(), entryId, BKException.getMessage(rc));
                } else {
                    metricBean.success(System.currentTimeMillis() - startTime);
                    log.info("ledger {} entry {} add entry success", lh.getId(), entryId);
                }
            });
        } catch (Exception e) {
            metricBean.fail(System.currentTimeMillis() - startTime);
            log.error("current ledger is {} send error ", rotateLedger.getCurrentLedgerId(), e);
        }
    }
}
