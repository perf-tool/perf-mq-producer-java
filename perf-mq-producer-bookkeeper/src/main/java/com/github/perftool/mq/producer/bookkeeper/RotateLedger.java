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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

import java.nio.charset.StandardCharsets;
import java.util.Random;

@Slf4j
public class RotateLedger {

    private final BookKeeper bookKeeper;

    private final BookkeeperConfig conf;

    private final Random random;

    private final int rotateSeconds;

    private final int randomSecondsOffset;

    private volatile LedgerHandle ledgerHandle;

    private long lastRotateTime;

    private long nextRotateTime;

    public RotateLedger(BookKeeper bookKeeper,
                        BookkeeperConfig conf) throws BKException, InterruptedException {
        this.bookKeeper = bookKeeper;
        this.conf = conf;
        this.random = new Random();
        this.rotateSeconds = conf.ledgerRotateSeconds;
        this.randomSecondsOffset = conf.ledgerRotateSeconds / 10;
        checkRotateLedger(conf);
    }

    public void send(byte[] bytes, AsyncCallback.AddCallback cb) throws Exception {
        checkRotateLedger(conf);
        if (this.ledgerHandle == null) {
            throw new Exception("Rotate ledger error, ledger handle is null");
        }
        ledgerHandle.asyncAddEntry(bytes, cb, null);
    }

    private void checkRotateLedger(BookkeeperConfig conf) {
        if (System.currentTimeMillis() < this.nextRotateTime) {
            return;
        }
        if (this.ledgerHandle != null) {
            long ledgerId = ledgerHandle.getId();
            try {
                this.ledgerHandle.close();
                bookKeeper.deleteLedger(ledgerId);
            } catch (Exception e) {
                log.error("ledger id {} close error", ledgerId, e);
            }
        }
        try {
            this.ledgerHandle = bookKeeper.createLedger(conf.ledgerEnsembleSize, conf.ledgerWriteQuorumSize,
                    conf.ledgerAckQuorumSize, BookKeeper.DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("create ledger error", e);
        }
        this.lastRotateTime = System.currentTimeMillis();
        this.nextRotateTime = lastRotateTime + rotateSeconds * 1000L + random.nextInt(randomSecondsOffset * 1000);
    }

    public long getCurrentLedgerId() {
        if (ledgerHandle == null) {
            return -1;
        }
        return ledgerHandle.getId();
    }

}
