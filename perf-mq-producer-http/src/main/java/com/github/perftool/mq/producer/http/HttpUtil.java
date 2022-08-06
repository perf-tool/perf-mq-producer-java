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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpUtil {

    public static String getHttpData() {
        return "{\n"
                + "      \"name\": \"Avengers: Endgame\",\n"
                + "      \"years\": 2019,\n"
                + "      \"like\": true,\n"
                + "      \"heroes\": [\n"
                + "        {\n"
                + "          \"name\": \"Tony Stark\",\n"
                + "          \"nickName\": \"Iron Man\",\n"
                + "          \"actor\": \"Robert Downey\",\n"
                + "          \"age\": 48\n"
                + "        }, {\n"
                + "          \"name\": \"Steve Rogers\",\n"
                + "          \"nickName\": \"Captain America\",\n"
                + "          \"actor\": \"Chris Evans\",\n"
                + "          \"age\": 100\n"
                + "        }],\n"
                + "      \"marvelInfo\": {\n"
                + "        \"sequence\": 22,\n"
                + "        \"hasIronMan\": true\n"
                + "      },\n"
                + "      \"null\": null\n"
                + "    }";
    }

}
