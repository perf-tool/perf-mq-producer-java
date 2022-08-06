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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class MqttConfig {

    @Value("${MQTT_HOST:localhost}")
    public String host;

    @Value("${MQTT_PORT:1883}")
    public int port;

    @Value("${CLIENT_ID:clientId}")
    public String clientId;

    @Value("${MQTT_TOPIC:topic}")
    public String topic;

    @Value("${MQTT_USERNAME:username}")
    public String username;

    @Value("${MQTT_PASSWORD:password}")
    public String password;

    @Value("${MQTT_PRODUCER_NUM_PER_THREAD:1}")
    public int producerNum;

    @Value("${MQTT_MESSAGE_BYTE:1024}")
    public int messageByte;

}
