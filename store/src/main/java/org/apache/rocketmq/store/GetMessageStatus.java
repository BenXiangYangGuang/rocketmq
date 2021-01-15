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
package org.apache.rocketmq.store;
// 查询ConsumeQueue中的消息结果
public enum GetMessageStatus {
    // 发现
    FOUND,
    // 没有匹配的消息
    NO_MATCHED_MESSAGE,
    // 消息存放在下一个commitlog文件
    MESSAGE_WAS_REMOVING,

    OFFSET_FOUND_NULL,

    OFFSET_OVERFLOW_BADLY,

    OFFSET_OVERFLOW_ONE,
    // 请求的offset 太小
    OFFSET_TOO_SMALL,

    NO_MATCHED_LOGIC_QUEUE,
    // 消息不在consumequeue中
    NO_MESSAGE_IN_QUEUE,
}
