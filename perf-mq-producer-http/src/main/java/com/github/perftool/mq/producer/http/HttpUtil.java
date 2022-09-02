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
        return """
                {
                      "name": "Avengers: Endgame",
                      "years": 2019,
                      "like": true,
                      "heroes": [
                        {
                          "name": "Tony Stark",
                          "nickName": "Iron Man",
                          "actor": "Robert Downey",
                          "age": 48
                        }, {
                          "name": "Steve Rogers",
                          "nickName": "Captain America",
                          "actor": "Chris Evans",
                          "age": 100
                        }],
                      "marvelInfo": {
                        "sequence": 22,
                        "hasIronMan": true
                      },
                      "null": null
                    }""";
    }

}
