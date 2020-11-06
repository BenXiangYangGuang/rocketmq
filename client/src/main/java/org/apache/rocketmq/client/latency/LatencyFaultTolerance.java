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

package org.apache.rocketmq.client.latency;

/**
 * Broker 故障延迟规避机制，存储的是有故障的条目(broker)
 * 每一次 Producer 发送消息时都会进行 MessageQueue 选择，如果一个 Broker 宕机了，每次 MessageQueue 的选择都会去选择这个 Broker 下的消息队列，
 * 直接规避这个 Broker，使这个 Broker 中的 MessageQueue 不在消息队列中，从而，减少了选择 MessageQueue 失败的次数。
 * @param <T>
 */
public interface LatencyFaultTolerance<T> {
    /**
     * 将此 broker 进行 notAvailableDuration 时间的隔离， 这段时间内 broker 为不可用
     * @param name brokerName
     * @param currentLatency 消息发送故障延迟时间
     * @param notAvailableDuration 不可用持续时长，在这个时间内，Broker 将被规避
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断 Broker 是否可用，不在进行隔离
     * @param name
     * @return
     */
    boolean isAvailable(final T name);

    /**
     * 移除已经被隔离的 broker，意味着 Broker 重新参与路由计算

     * @param name
     */
    void remove(final T name);

    /**
     * 尝试从规避的 Broker 中选择一个可用 Broker，如果没有找到，将返回 null
     * @return
     */
    T pickOneAtLeast();
}
