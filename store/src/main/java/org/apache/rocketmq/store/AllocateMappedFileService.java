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

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * 提前创建 MappedFile 文件的服务，继承 ServiceThread，实现了 Runnable，一个单独的线程
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 等待创建映射文件的超时时间，默认5秒
    private static int waitTimeOut = 1000 * 5;
    // 请求队列存储集合
    // 用来保存当前所有待处理的分配请求，key 是filePath，value 是分配请求。如果分配请求被成功处理，即获取到映射文件则从 requestTable 移除这个请求。
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<String, AllocateRequest>();
    // 分配请求队列，包含优先级，从该队列中获取请求，进而根据请求创建映射文件
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
        new PriorityBlockingQueue<AllocateRequest>();
    // 标识分配 mappedFile 时是否发生异常
    private volatile boolean hasException = false;
    // 默认消息存储
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 预先创建 MappedFile 文件，只是先创建2个创建mappedFile 文件的请求，放入队列中，具体 mappedFile 文件的创建和文件内存直接映射由 mmapOperation() 方法来实现。
     * @param nextFilePath 创建 mappedFile 文件的全路径名称
     * @param nextNextFilePath 创建下一个 mappedFile 文件的全路径名称
     * @param fileSize 文件大小
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        // 默认提交两个请求
        int canSubmitRequests = 2;
        // 是否启用 transientStorePool
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            // SLAVE 节点中 transientByteBuffer 即使没有足够的 ByteBuffer，也不支持快速失败
            // 启动快速失败策略时，计算TransientStorePool中剩余的buffer数量减去requestQueue中待分配的数量后，剩余的buffer数量
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                // 可用的 ByteBuffer - requestQueue，还剩余可用的 ByteBuffer 数量
                canSubmitRequests = this.messageStore.getTransientStorePool().availableBufferNums() - this.requestQueue.size();
            }
        }
        // 创建分配请求
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        // 判断requestTable中是否存在该路径的分配请求，如果存在则说明该请求已经在排队中
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;
        //该路径没有在排队
        if (nextPutOK) {
            // byteBuffer 数量不够，则快速失败
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            // 数量充足的话，将指定的元素插入到此优先级队列中
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            // 请求数量 -1
            canSubmitRequests--;
        }
        // 下下个请求的处理
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }
        // 报错，日志
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }
        // 下一个分配请求，获取当前请求，然后通过线程协调器CountDownLatch，协调另一个线程进行完mmpOperation操作后，返回创建好的MappedFile文件
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                // 默认等待5s，等待 mmapOperation 操作创建 mappedFile
                // 调用此方法的线程会被阻塞，直到 CountDownLatch 的 count 为 0；等到 mmapOperation() finally countDownLatch 为 0
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    // 成功从 requestTable 中移除请求，并返回 mappedFile 文件
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    /**
     * 服务类名称
     * @return
     */
    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    /**
     * 停止 mappedFile 文件分配服务
     */
    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                // 销毁 mappedFile
                req.mappedFile.destroy(1000);
            }
        }
    }
    /**
     * 开始 mappedFile 文件分配服务，从 requestQueue 中获取创建 mappedFile 的文件请求
     */
    public void run() {
        log.info(this.getServiceName() + " service started");
        // 除非停止，否则一直在进行 mmap 映射操作
        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     *
     * 从 requestQueue 中获取 MappedFile 创建请求，并创建 mappedFile，及创建好 mappedFile 和直接内存 MappedByteBuffer 的映射
     * 在消息写入过程中（调用CommitLog的putMessage()方法），CommitLog会先从MappedFileQueue队列中获取一个 MappedFile，如果没有就新建一个。
     * 这里，MappedFile的创建过程是将构建好的一个AllocateRequest请求（具体做法是，将下一个文件的路径、下下个文件的路径、文件大小为参数封装为AllocateRequest对象）添加至队列中，
     * 后台运行的AllocateMappedFileService服务线程（在Broker启动时，该线程就会创建并运行），会不停地run，只要请求队列里存在请求，就会去执行MappedFile映射文件的创建和预分配工作，
     * 分配的时候有两种策略，一种是使用Mmap的方式来构建MappedFile实例，
     * 另外一种是从TransientStorePool堆外内存池中获取相应的DirectByteBuffer来构建MappedFile（ps：具体采用哪种策略，也与刷盘的方式有关,异步刷盘才会开启 transientStorePool，才会采用 transientStorePool 中的 writerBuffer，构造 mappedFile 文件）。
     * 并且，在创建分配完下个MappedFile后，还会将下下个MappedFile预先创建并保存至请求队列中等待下次获取时直接返回。
     * RocketMQ中预分配MappedFile的设计非常巧妙，下次获取时候直接返回就可以不用等待MappedFile创建分配所产生的时间延迟。
     *
     * 只有被其他线程中断，才会返回 false
     * Only interrupted by the external thread, will return false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            // 从队列取出 AllocateRequest
            req = this.requestQueue.take();
            // requestTable 中获取 AllocateRequest
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            // 这个请求不存在的log
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }
            // 是否是同一个 AllocateRequest
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                // 是否开启 transientStorePool 中的直接内存映射
                // 开启 transientStorePool 暂时缓存池

                // TransientStorePool与MappedFile在数据处理上的差异在什么地方呢？
                // 分析其代码，TransientStorePool会通过ByteBuffer.allocateDirect调用直接申请对外内存，消息数据在写入内存的时候是写入预申请的内存中。
                // 在异步刷盘的时候，再由刷盘线程将这些内存中的修改写入文件。
                // 那么与直接使用MappedByteBuffer相比差别在什么地方呢？
                // MappedByteBuffer 和 WriteBuffer 都会经过，PageCache 这个操作进行写入磁盘。
                // MappedByteBuffer写入数据，写入的是MappedByteBuffer映射的磁盘文件对应的Page Cache，可能会慢一点。
                // 而TransientStorePool方案下写入的则为纯粹的内存，并不是PageCache，因此在消息写入操作上会更快，因此能更少的占用CommitLog.putMessageLock锁，从而能够提升消息处理量。然后再经过刷盘将直接内存中的数据经过Page Cache 写入磁盘。
                // 使用TransientStorePool方案的缺陷主要在于在异常崩溃的情况下回丢失更多的消息。


                // MMap的写入操作是：Mmap的MappedByteBuffer映射直接内存，直接内存映射文件，然后文件会对应Page Cache，也就是 MmapedByteBuffer的直接内存可能是Page Cache的东西，然后通过写Page Cache，然后再写入磁盘。
                // FileChannle:是写直接内存，这个效率比较高，然后直接内存满了，在落盘的时候，再去经过Page Cache，落入磁盘。
                // WriterBuffer的写入方式实际也就是FileChannel的写入方式，Mmap在写入4k一下的文件比较快，然后FileChannel写入文件大于4k时，比Mmap方式的要快，可能是因为PageCache 是4k，然后写着就可能去落盘了。
                // 而FileChannel 是写满了直接内存，才去经过PageCache，这样写入直接内存的效率更高，然后再经过Page Cache，当大于4k的时候，大于Page Cache的内存的时候，就是FileChannel快了。大概因为FileChannel是基于Block（块）的。
                // https://juejin.cn/post/6844903842472001550

                // https://www.cnkirito.moe/file-io-best-practise/
                // https://www.dazhuanlan.com/2019/11/05/5dc0f35b9a621/
                // https://blog.csdn.net/alex_xfboy/article/details/90174840

                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        // 为每一个 mappedFile 文件，进行init中的mmap 映射操作
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        // spi 加载失败，使用构造方法创建 mappedFile
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    // 不开启 transientStorePool，直接内存映射
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }
                // 消耗的时间
                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                // log
                if (elapsedTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
                        + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                // 通过 mmap 建立内存映射仅是将文件磁盘地址和虚拟地址通过映射对应起来，此时物理内存并没有填充磁盘文件内容。
                // 当实际发生文件读写时，产生缺页中断并陷入内核，然后才会将磁盘文件内容读取至物理内存。针对上述场景，RocketMQ 设计了 MappedFile 预热机制。
                // 当 RocketMQ 开启 MappedFile 内存预热（warmMapedFileEnable），且 MappedFile 文件映射空间大小大于等于 mapedFileSizeCommitLog（1 GB） 时，调用 warmMappedFile 方法对 MappedFile 进行预热。

                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                    .getMappedFileSizeCommitLog()
                    &&
                    this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    // 对 mappedFile 进行预热
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                        this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }
                // 设置创建 mappedFile 的结果文件
                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            // 创建失败，再将请求入队，等待 1 毫秒，再处理下一个请求
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            // 最终释放 AllocateRequest 请求
            if (req != null && isSuccess)
                // 同步等待线程，解除这个 request 的请求
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    /**
     * 分配 MappedFile 文件请求
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        // 文件路径
        private String filePath;
        // 文件大小
        private int fileSize;
        // 程序计数器，可以在 countDownLatch >  0 时，其他线程去执行其他操作。
        // 一个同步线程工具，让其他线程完成一定操作，这个线程再进行下面操作；
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
