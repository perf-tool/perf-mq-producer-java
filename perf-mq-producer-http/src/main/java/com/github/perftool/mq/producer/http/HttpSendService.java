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

package com.github.perftool.mq.producer.http;

import com.github.perftool.mq.producer.common.AbstractProduceThread;
import com.github.perftool.mq.producer.common.config.ThreadConfig;
import com.github.perftool.mq.producer.common.metrics.MetricBean;
import com.github.perftool.mq.producer.common.metrics.MetricFactory;
import com.github.perftool.mq.producer.common.module.OperationType;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.nio.AsyncClientConnectionManager;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;

@Slf4j
public class HttpSendService extends AbstractProduceThread {

    private final HttpConfig httpConfig;

    private CloseableHttpAsyncClient client;

    private final MetricBean metricBean;

    public HttpSendService(int index, MetricFactory metricFactory, ThreadConfig threadConfig, HttpConfig httpConfig) {
        super(index, metricFactory, threadConfig);
        this.httpConfig = httpConfig;
        this.metricBean = newMetricBean(OperationType.PRODUCE);
    }

    @Override
    public void init() throws Exception {
        final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setSoTimeout(Timeout.ofSeconds(5))
                .build();

        final AsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnPerRoute(1000).setMaxConnTotal(2000).build();

        client = HttpAsyncClients.custom()
                .setConnectionManager(connectionManager)
                .setIOReactorConfig(ioReactorConfig)
                .build();

        client.start();
    }

    @Override
    protected void send() {
        long startTime = System.currentTimeMillis();
        try {
            HttpHost host = new HttpHost(httpConfig.httpHost, httpConfig.httpPort);
            SimpleHttpRequest simpleHttpRequest = new SimpleHttpRequest("POST", host, "/echo");
            simpleHttpRequest.setBody(HttpUtil.getHttpData(), ContentType.APPLICATION_JSON);
            client.execute(simpleHttpRequest, new FutureCallback<>() {
                @Override
                public void completed(SimpleHttpResponse simpleHttpResponse) {
                    if (simpleHttpResponse.getCode() >= 200 && simpleHttpResponse.getCode() > 200) {
                        metricBean.success(System.currentTimeMillis() - startTime);
                    } else {
                        metricBean.fail(System.currentTimeMillis() - startTime);
                    }
                    log.info("http request success, response code is [{}]", simpleHttpResponse.getCode());
                }

                @Override
                public void failed(Exception e) {
                    metricBean.fail(System.currentTimeMillis() - startTime);
                    log.error("send error is ", e);
                }

                @Override
                public void cancelled() {

                }
            });
        } catch (Exception e) {
            metricBean.fail(System.currentTimeMillis() - startTime);
            log.error("send req exception ", e);
        }
    }
}
