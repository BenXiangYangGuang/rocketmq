/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer;

public enum PullStatus {
    /**
     * 发现
     * Founded
     */
    FOUND,
    /**
     * 没有新的消息被拉取
     * No new message can be pull
     */
    NO_NEW_MSG,
    /**
     * 过滤结果不匹配
     * Filtering results can not match
     */
    NO_MATCHED_MSG,
    /**
     * 不合法的offset
     * Illegal offset,may be too big or too small
     */
    OFFSET_ILLEGAL
}
