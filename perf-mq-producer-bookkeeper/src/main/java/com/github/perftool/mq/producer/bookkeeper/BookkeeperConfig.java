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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class BookkeeperConfig {

    @Value("${BOOKKEEPER_ZOOKEEPER_SERVERS:localhost:2181}")
    public String servers;

    @Value("${BOOKKEEPER_NUM_IO_THREADS:1}")
    public int ioThreads;

    @Value("${BOOKKEEPER_NUM_WORKER_THREADS:1}")
    public int workerThreads;

    @Value("${BOOKKEEPER_LEDGER_NUM_PRE_THREAD:1}")
    public int ledgerNum;

    @Value("${BOOKKEEPER_LEDGER_ROTATE_SECONDS:300}")
    public int ledgerRotateSeconds;

    @Value("${BOOKKEEPER_LEDGER_ENSEMBLE_SIZE:1}")
    public int ledgerEnsembleSize;

    @Value("${BOOKKEEPER_LEDGER_WRITE_QUORUM_SIZE:1}")
    public int ledgerWriteQuorumSize;

    @Value("${BOOKKEEPER_LEDGER_ACK_QUORUM_SIZE:1}")
    public int ledgerAckQuorumSize;

    @Value("${BOOKKEEPER_ADD_ENTRY_TIMEOUT_SECONDS:30}")
    public int addEntryTimeoutSeconds;

    @Value("${BOOKKEEPER_READ_ENTRY_TIMEOUT_SECONDS:30}")
    public int readEntryTimeoutSeconds;

    @Value("${BOOKKEEPER_NUM_CHANNELS_PER_BOOKIE:4}")
    public int channelsPerBookie;

    @Value("${BOOKKEEPER_USE_V2_WIRE_PROTOCOL:false}")
    public boolean useV2WireProtocol;

    @Value("${BOOKKEEPER_MESSAGE_BYTE:1024}")
    public int messageByte;
}
