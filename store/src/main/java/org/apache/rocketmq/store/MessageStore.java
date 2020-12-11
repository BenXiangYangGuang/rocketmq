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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * This class defines contracting interfaces to implement, allowing third-party vendor to use customized message store.
 */
public interface MessageStore {

    /**
     * 加载原来存储的消息
     * Load previously stored messages.
     *
     * @return true if success; false otherwise.
     */
    boolean load();

    /**
     * 启动消息存储
     * Launch this message store.
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * 关闭消息存储
     * Shutdown this message store.
     */
    void shutdown();

    /**
     * 销毁消息存储
     * Destroy this message store. Generally, all persistent files should be removed after invocation.
     */
    void destroy();

    /**
     * 异步存储消息，不用等待结果，处理下一个异步请求
     * Store a message into store in async manner, the processor can process the next request
     *  rather than wait for result
     *  when result is completed, notify the client in async manner
     *
     * @param msg MessageInstance to store
     * @return a CompletableFuture for the result of store operation
     */
    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        return CompletableFuture.completedFuture(putMessage(msg));
    }

    /**
     * 异步存储消息，存储完毕再通知客户端
     * Store a batch of messages in async manner
     * @param messageExtBatch the message batch
     * @return a CompletableFuture for the result of store operation
     */
    default CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        return CompletableFuture.completedFuture(putMessages(messageExtBatch));
    }

    /**
     * 存储消息到磁盘文件
     * Store a message into store.
     *
     * @param msg Message instance to store
     * @return result of store operation.
     */
    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    /**
     * 存储批量消息到磁盘文件
     * Store a batch of messages.
     *
     * @param messageExtBatch Message batch.
     * @return result of storing batch messages.
     */
    PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

    /**
     * 查询一个消息
     * Query at most <code>maxMsgNums</code> messages belonging to <code>topic</code> at <code>queueId</code> starting
     * from given <code>offset</code>. Resulting messages will further be screened using provided message filter.
     *h
     * @param group Consumer group that launches this query.
     * @param topic Topic to query.
     * @param queueId Queue ID to query.
     * @param offset Logical offset to start from.
     * @param maxMsgNums Maximum count of messages to query.
     * @param messageFilter Message filter used to screen desired messages.
     * @return Matched messages.
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * 获取一个topic，一个消息队列的最大offset
     * Get maximum offset of the topic queue.
     *
     * @param topic Topic name.
     * @param queueId Queue ID.
     * @return Maximum offset at present.
     */
    long getMaxOffsetInQueue(final String topic, final int queueId);

    /**
     *
     * 获取一个topic，一个消息队列的最小的offset
     *
     * Get the minimum offset of the topic queue.
     *
     * @param topic Topic name.
     * @param queueId Queue ID.
     * @return Minimum offset at present.
     */
    long getMinOffsetInQueue(final String topic, final int queueId);

    /**
     * 根据 consumeQueue 中的 offset，获取 commitlog 中物理的 offset
     * Get the offset of the message in the commit log, which is also known as physical offset.
     *
     * @param topic Topic of the message to lookup.
     * @param queueId Queue ID.
     * @param consumeQueueOffset offset of consume queue.
     * @return physical offset.
     */
    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 查询物理的 offset ，根据指定的时间戳
     * Look up the physical offset of the message whose store timestamp is as specified.
     *
     * @param topic Topic of the message.
     * @param queueId Queue ID.
     * @param timestamp Timestamp to look up.
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    /**
     * 根据commitlog 中的物理offset 查找消息
     * Look up the message by given commit log offset.
     *
     * @param commitLogOffset physical offset.
     * @return Message whose physical offset is as specified.
     */
    MessageExt lookMessageByOffset(final long commitLogOffset);

    /**
     * 根据一个 commitLogOffset ，获取一个消息
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    /**
     * 根据一个 commitLogOffset ，获取一个消息
     *
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @param msgSize message size.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    /**
     * 获取这个运行的store的信息
     * Get the running information of this store.
     *
     * @return message store running info.
     */
    String getRunningDataInfo();

    /**
     * running store 统计信息，以键值对的方式返回
     * Message store runtime information, which should generally contains various statistical information.
     *
     * @return runtime information of the message store in format of key-value pairs.
     */
    HashMap<String, String> getRuntimeInfo();

    /**
     * 获取最大的 commitlog 的 offset
     * Get the maximum commit log offset.
     *
     * @return maximum commit log offset.
     */
    long getMaxPhyOffset();

    /**
     * 获取最小的 commitlog 的 offset
     * Get the minimum commit log offset.
     *
     * @return minimum commit log offset.
     */
    long getMinPhyOffset();

    /**
     * 获取一个消息队列最早的存储时间
     * Get the store time of the earliest message in the given queue.
     *
     * @param topic Topic of the messages to query.
     * @param queueId Queue ID to find.
     * @return store time of the earliest message.
     */
    long getEarliestMessageTime(final String topic, final int queueId);

    /**
     * 获取一个消息队列最早的存储时间
     *
     * Get the store time of the earliest message in this store.
     *
     * @return timestamp of the earliest message in this store.
     */
    long getEarliestMessageTime();

    /**
     * 获取一个消息的存储时间
     * Get the store time of the message specified.
     *
     * @param topic message topic.
     * @param queueId queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     */
    long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 获取一个消息队列消息的总数量
     * Get the total number of the messages in the specified queue.
     *
     * @param topic Topic
     * @param queueId Queue ID.
     * @return total number.
     */
    long getMessageTotalInQueue(final String topic, final int queueId);

    /**
     * 根据 offset 获取一个原生的 commitlogdata，用来做数据复制的目的
     * Get the raw commit log data starting from the given offset, which should used for replication purpose.
     *
     * @param offset starting offset.
     * @return commit log data.
     */
    SelectMappedBufferResult getCommitLogData(final long offset);

    /**
     * 添加一个数据到 commitlog 中
     * Append data to commit log.
     *
     * @param startOffset starting offset.
     * @param data data to append.
     * @return true if success; false otherwise.
     */
    boolean appendToCommitLog(final long startOffset, final byte[] data);

    /**
     * 数据手动删除
     * Execute file deletion manually.
     */
    void executeDeleteFilesManually();

    /**
     * 根据 key 查询消息
     * Query messages by given key.
     *
     * @param topic topic of the message.
     * @param key message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin begin timestamp.
     * @param end end timestamp.
     */
    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
        final long end);

    /**
     * 更新 Slave 的 broker 的 Master 地址
     * Update HA master address.
     *
     * @param newAddr new address.
     */
    void updateHaMasterAddress(final String newAddr);

    /**
     * 返回跟随 Master 的 Slave 的broker 的数量
     * Return how much the slave falls behind.
     *
     * @return number of bytes that slave falls behind.
     */
    long slaveFallBehindMuch();

    /**
     * 返回 store 当前的时间戳
     * Return the current timestamp of the store.
     *
     * @return current time in milliseconds since 1970-01-01.
     */
    long now();

    /**
     * 清除没有使用的 topic
     * Clean unused topics.
     *
     * @param topics all valid topics.
     * @return number of the topics deleted.
     */
    int cleanUnusedTopic(final Set<String> topics);

    /**
     * 清除过期的 consume queue
     * Clean expired consume queues.
     */
    void cleanExpiredConsumerQueue();

    /**
     * 检测消息是否被从内存中删除
     *
     * Check if the given message has been swapped out of the memory.
     *
     * @param topic topic.
     * @param queueId queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is no longer in memory; false otherwise.
     */
    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * 获取已经被 commitlog 存储，还有被分发到 consume queue 的字节数量
     * Get number of the bytes that have been stored in commit log and not yet dispatched to consume queue.
     *
     * @return number of the bytes to dispatch.
     */
    long dispatchBehindBytes();

    /**
     * 刷新数据到持久的数据中
     * Flush the message store to persist all data.
     *
     * @return maximum offset flushed to persistent storage device.
     */
    long flush();

    /**
     * 重置写入的 offset
     * Reset written offset.
     *
     * @param phyOffset new offset.
     * @return true if success; false otherwise.
     */
    boolean resetWriteOffset(long phyOffset);

    /**
     * 获取被确认的 offset
     * Get confirm offset.
     *
     * @return confirm offset.
     */
    long getConfirmOffset();

    /**
     *
     * 设置一个被确认的 offset
     * Set confirm offset.
     *
     * @param phyOffset confirm offset to set.
     */
    void setConfirmOffset(long phyOffset);

    /**
     * 检测一个页缓存是否忙
     * Check if the operation system page cache is busy or not.
     *
     * @return true if the OS page cache is busy; false otherwise.
     */
    boolean isOSPageCacheBusy();

    /**
     * 检测 store 被锁了多长时间 毫秒
     * Get lock time in milliseconds of the store by far.
     *
     * @return lock time in milliseconds.
     */
    long lockTimeMills();

    /**
     * 检测瞬时的存储池是否充足的
     * Check if the transient store pool is deficient.
     *
     * @return true if the transient store pool is running out; false otherwise.
     */
    boolean isTransientStorePoolDeficient();

    /**
     * 获取一个分发List
     * Get the dispatcher list.
     *
     * @return list of the dispatcher.
     */
    LinkedList<CommitLogDispatcher> getDispatcherList();

    /**
     * 获取一个消费队列
     * Get consume queue of the topic/queue.
     *
     * @param topic Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueue getConsumeQueue(String topic, int queueId);

    /**
     * 获取 messageStore 的管理者
     * Get BrokerStatsManager of the messageStore.
     *
     * @return BrokerStatsManager.
     */
    BrokerStatsManager getBrokerStatsManager();

    /**
     * 处理定时消息服务
     * handle
     * @param brokerRole
     */
    void handleScheduleMessageService(BrokerRole brokerRole);
}
