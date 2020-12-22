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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;
// HAConnection是Master用来和Slave建立连接的类，处理和Slave的交互；ReadSocketService用来读取Slave向Master发送的数据，WriteSocketService用来写Master向Slave返回的数据；
// Master（类似服务端）Slave（类似客户端）
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final HAService haService;
    // Master和Slave建立的socket通道
    private final SocketChannel socketChannel;
    private final String clientAddr;
    // ReadSocketService用来读取Slave向Master发送的数据
    private WriteSocketService writeSocketService;
    // WriteSocketService用来写Master向Slave返回的数据
    private ReadSocketService readSocketService;
    // slave请求master的offset
    private volatile long slaveRequestOffset = -1;
    // master接受到slave发送的offset
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        // 非阻塞通道
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        // 连接建立更新连接数量
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }
    // ReadSocketService用来读取Slave向Master发送的数据，采用IO复用的方式处理
    class ReadSocketService extends ServiceThread {
        // 1M
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        // 和Slave建立的Socket通道
        private final SocketChannel socketChannel;

        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // ReadSocketService处理byteBufferRead读取到的位置
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");
            // 线程不会停止
            while (!this.isStopped()) {
                try {
                    // 同步轮询SocketChannel，等待IO事件通知完成，超时等待1秒
                    this.selector.select(1000);
                    // Master处理Slave发送的offset请求，并返回
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    // Master和Slave连接超时间隔，20秒超时，记录log
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
            // Broker停机，线程关闭，资源释放
            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }
        // Master处理Slave发送的offset请求，并返回
        private boolean processReadEvent() {
            // 读取到数据为0byte的数据次数
            int readSizeZeroTimes = 0;
            // byteBufferRead不在包含空余空间，进行重新开启
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }
            // byteBufferRead还有剩余空间
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 读取数据到byteBufferRead中
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    // 读取到数据
                    if (readSize > 0) {
                        // 更新readSizeZeroTimes和lastReadTimestamp
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        // byteBufferRead中读取到的数据位置>byteBufferRead上次处理过的数据>8;
                        // 读取超过8byte：8byte:代表slave向master发送的offset的大小8byte
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            // 获得slave发送的最大的offset的位置
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 读取offset
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            // 更新处理的位置
                            this.processPosition = pos;
                            // master接受到slave发送的offset
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }
                            // 唤醒GroupTransferService#WaitNotifyObject#notifyTransferObject判断这个offset是否发送了，没有发送进行等待(GroupTransferService#notifyTransferObject.waitForRunning(1000))，
                            // 等待WriteSocketService写数据成功，然后再判断是否写入成功。
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    //    读取到数据的数据大小为0，3次跳出循环
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }
    // WriteSocketService用来Master向Slave写返回的数据（commitlog的message数据）；
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;
        // 8byte:offset大小+4字节消息大小
        private final int headerSize = 8 + 4;
        // 消息头Buffer
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        // 下次master转移到slave开始的位置
        private long nextTransferFromWhere = -1;
        private SelectMappedBufferResult selectMappedBufferResult;
        // 上次写slave的数据是否完成
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 同步轮询SocketChannel，等待IO事件通知完成，超时等待1秒
                    this.selector.select(1000);
                    // slave请求master的offset == -1，项目刚开始启动，master未接收到slave的拉取请求，sleep
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }
                    // nextTransferFromWhere = -1说明第一次进行数据传输，需要计算传输的物理偏移量
                    if (-1 == this.nextTransferFromWhere) {
                        // 如果slaveRequestOffset为0则从当前最后一个commitlog文件传输，否则根据slave broker的拉取请求偏移量开始
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // 确定Master的offset
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 下次开始位置为slave请求位置
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }
                    // 上次写slave数据完成
                    if (this.lastWriteOver) {
                        // 距上次写数据间隔
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    //  上次传输未结束则继续传输，可能是byteBufferHeader有剩余，也可能是SelectMappedBufferResult.ByteBuffer盛放消息的具体内容的数据还有剩余，没有被写完，重新开始写
                    } else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }
                    // 根据offset从master的commitlog文件获取数据
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();
                        // 向slave的socket通道写数据
                        this.lastWriteOver = this.transferData();
                    } else {
                        // 如果没有获取到commitlog的数据，则进行等待；
                        // 一个Slave到Master的连接，一个HAConnection对象，一个WriteSocketService对象，一个线程，
                        // 因为Master没有最新的commitlog的数据，所以把所有的等待着数据的HAConnection的WriteSocketService()动作，进行等待；
                        // 将所有的HAConnection的WriteSocketService()线程被设置为未被通知的状态
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
            // 正常关机
            // 将这个连接线程关闭，移除
            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }
        // 向slave的socket通道写数据
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            // 如果读到Header数据的大小为0byte>3,跳出这个循环，进行下次header的写入
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }
            // 根据offset从master的commitlog文件获取数据，maser是否有数据
            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            // header被写满，开始写body；header：offset大小+4字节消息大小；header写满了，一定会有message的body，再去小body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }
            // header已经被写满 + selectMappedBufferResult里面存储message的内容的ByteBuffer已经被写完了，那这次写数据成功了。
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();
            // 释放空间
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
