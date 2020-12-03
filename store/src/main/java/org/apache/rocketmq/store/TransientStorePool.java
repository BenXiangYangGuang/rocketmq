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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 短暂的存储池。RocketMQ 单独创建一个直接内存缓存池，用来临时存储数据，数据先写入该内存映射中，然后由 commit 线程定时将数据从该内存提交到对应的FileChannel中。
 *
 * 如果没有使用 transientStorePool，则不存在 commit 过程。直接将数据映射到了 MappedByteBuffer 中，然后进行刷盘操作。所以说 commit 操作是对使用了 transientStorePool 中的临时直接内存 ByteBuffer 来说的。
 *
 * 是否启用 TransientStorePool，条件为 transientStorePoolEnable = true && 异步刷盘 && 主节点
 *
 * RocketMQ 引入该机制的主要原因是提供了一种内存锁定，将当直接内存一直锁定在内存中，避免被进程将内存交换到磁盘。
 * 创建poolSize 个堆外内存，并利用 com.sun.jna.Library 类库将该批内存锁定，避免被置换到交换区，提高存储性能。
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // availableBuffers 个数，可通过在broker中配置文件中设置 transientStorePool，默认值为 5
    private final int poolSize;
    // 每个 ByteBuffer 大小，默认为 mappedFileSizeCommitLog，表明 TransientStorePool 为 commitlog 文件服务
    private final int fileSize;
    // 直接内存，ByteBuffer 容器，双端队列
    private final Deque<ByteBuffer> availableBuffers;
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * 创建默认的堆外内存
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            // 利用 NIO 直接直接分配，堆外内存（直接内存），在系统中的内存，非 JVM 内存
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
            // 内存地址
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 内存锁定
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁 byteBuffer,只是解锁内存，具体的 writerBuffer 会在 commit 之后，为 null，然后 mappedFile 持有 transientStorePool 对象。
     * mappedFile 被销毁之后，transientStorePool 也会被销毁，availableBuffers 也会被销毁
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 解锁内存
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 归还堆外内存，设置起始 byteBuffer 起始位置，设置 byteBuffer 大小 1G； 1024 * 1024 * 1024
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 取出一个堆外内存，
     * @return
     */
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        // 可用数量小于 poolSize * 0.4，写一个数量剩余 log
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * availableBuffers 数量大小
     * @return
     */
    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
