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

import java.util.concurrent.atomic.AtomicLong;

/**
 * 引用资源：mappedFile 文件被引用的情况
 */
public abstract class ReferenceResource {
    // 引用次数
    protected final AtomicLong refCount = new AtomicLong(1);
    // 是否可用
    protected volatile boolean available = true;
    // 是否被清除
    protected volatile boolean cleanupOver = false;
    // 第一次关闭时间
    private volatile long firstShutdownTimestamp = 0;

    /**
     * mappedFile 文件是否被持有
     * @return
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                // reCount == 0 ,次数 - 1，变为 -1，并返回 false
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * mappedFile 关闭
     * @param intervalForcibly 表示拒绝被销毁的最大存活时间
     */
    public void shutdown(final long intervalForcibly) {
        // 如果可用
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        // 如果不可用，还有引用
        } else if (this.getRefCount() > 0) {
            // 表示
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放引用，引用次数为 0 时，清除这个 mappedByteBuffer，还要 mappedFile 文件个数 - 1，JVM 映射的直接内存  - fileSize
     */
    public void release() {
        // 当前file引用个数-1
        long value = this.refCount.decrementAndGet();
        // > 0，返回；否则清除file
        if (value > 0)
            return;
        // 清除mappedFile的mappedByteBuffer，减少内存空间统计
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    /**
     * 清除 mappedByteBuffer
     * @param currentRef
     * @return
     */
    public abstract boolean cleanup(final long currentRef);

    /**
     * @return mappedByteBuffer 是否被清除
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
