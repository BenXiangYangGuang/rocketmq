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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;
//Master和Slave通信的服务类；包含Slave作为客户端的HAClient类对象；
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 建立连接数量
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    // Master和Slave建立的HAConnection的集合
    private final List<HAConnection> connectionList = new LinkedList<>();
    // 负责和Slave建立Socket连接，并封装成HAConnection对象
    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;
    // WaitNotifyObject是用来管理多个Master和Slave的HAConnection的，唤醒WriteSocketService中Master向Slave写CommitLog数据的，
    // waitNotifyObject 协调GroupTransferService同步消息GroupCommitRequest 与 WriteSocketService写commitlog线程之间通信的；
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    // 推送的slave的最大的offset
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);
    //
    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * 推送消息的offset>master记录的最大的offset，且推送消息小于256M，返回ok
     * @param masterPutWhere 此次一个消息同步到slave的master消息的offset
     * @return
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        // 连接是否ok
        boolean result = this.connectionCount.get() > 0;
        // 推送消息的offset>master记录的最大的offset，且推送消息小于256M，返回ok
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 唤醒Master的commitlog推送服务，将commitlog的message发送给slave
     * @param offset slave发送的它接收到的最大的offset
     */
    public void notifyTransferSome(final long offset) {
        // 只要新要求的offset大于上次推送的offset，就进行for循环
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // 原子更新push2SlaveMaxOffset的值
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                // 成功，通知groupTransferService唤醒commitlog消息同步服务
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                // 失败再次获取推送的slave的最大的offset
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }


    // 启动Master和Slave数据同步服务
    public void start() throws Exception {
        // 建立和Slave的socket连接，监听端口
        this.acceptSocketService.beginAccept();
        // 开始监听
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }
    // WaitNotifyObject是用来管理多个线程被等待、唤醒等操作的对象
    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Master服务端和Slave建立连接，并监听Slave的IO事件，建立HAConnection对象。
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        // Master监听地址
        private final SocketAddress socketAddressListen;
        // socket通道
        private ServerSocketChannel serverSocketChannel;
        // NIO选择器
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            // 绑定监听
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 非阻塞
            this.serverSocketChannel.configureBlocking(false);
            // serverSocketChannel注册到selector
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            // 线程未停止
            while (!this.isStopped()) {
                try {
                    // 等待监听Socket的I/0完成事件通知，超时等待1秒
                    this.selector.select(1000);
                    // 被注册到selector上的key，也就是IO的socket
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        // 遍历监听，一个监听事件一个HAConnection
                        for (SelectionKey k : selected) {
                            // 监听状态ok
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                // 获取SocketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 建立和Slave的连接，里面包含了ReadSocketService用来读取Slave向Master发送的数据，WriteSocketService用来写Master向Slave返回的数据；
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // 开启ReadSocketService和WriteSocketService服务，处理Slave发来的请求和返回给Slave的数据
                                        conn.start();
                                        // 连接添加到集合
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }
                        // 清空selected
                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService是用来控制Master是否向Slave同步commitlog数据的。通过WaitNotifyObject来唤醒HAConnection中WriteSocketService向Slave写commitlog数据，同步等待5秒进行判断是否写入Slave是否成功。
     * Master和Slave会进行通信，Master写message到内存ByteBuffer，然后调用handleHA()方法，然后构造一个同步请求放入GroupTransferService#requestsWrite的队列里，
     * 等待HAConnection#WriteSocketService处理这个请求,然后将commitlog的message数据，同步到Slave中。
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {
        // 用来协调HAConnection中WriteSocketService和ReadSocketService之间的通信的
        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        // 写请求队列，两个队列进行交换
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        // 读请求队列，两个队列进行交换
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();
        // 放入请求到写队列
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            // 唤醒处理这个Request的线程，唤醒doWaitTransfer()方法
            this.wakeup();
        }
        // 通知Master的WriteSocketService给Slave传输一些数据
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }
        // 交换队列
        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // true，代表这个offset已经被推送过Slave了
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        // request被处理的截止时间，消息从Master同步到Slave的同步等待时间5秒；
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        // offset没有被推送过&&now<被处理的截止时间
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            // WaitNotifyObject对象的waitForRunning(),交换读写队列，转变对象为未被通知的状态，并等待1秒，
                            // 等待WriteSocketService中将数据写入到Slave中，并更细push2SlaveMaxOffset，表示已经发送；具体发送动作在WriteSocketService中，这里只有一个判断是否发送成功，然后是等待，等待发送结果。
                            this.notifyTransferObject.waitForRunning(1000);
                            // push2SlaveMaxOffset被更新，大于request的offset，表示被Slave处理成功。
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }
                        // 唤醒等待这个request处理结果的线程，应答存放这个request的线程，并返回结果；返回点为HandleHA()方法
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 父类ServiceThread的waitForRunning()方法，设置hasNotified为false，未被通知，然后交换写对队列和读队列，重置waitPoint为（1），休息200ms，finally设置hasNotified为未被通知，交换写对队列和读队列
                    this.waitForRunning(10);
                    //
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }
    // Slave向Master建立连接，发送Offset数据请求，并处理Master返回请求的类对象；HAClient作为Slave向Master通信的客户端，和Master建立socket连接。
    class HAClient extends ServiceThread {
        // 4M，用来存储Master返回的messag消息
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        // Slave向Master发送的offset
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        // Slave连接Master的socket通道
        private SocketChannel socketChannel;
        // 注册socketChannel的轮训的Selector
        private Selector selector;
        // 上次broker向Master发送offset的时间
        private long lastWriteTimestamp = System.currentTimeMillis();
        // 当前Slave最大的offset，表示slave当前向master已经发送了的offset
        private long currentReportedOffset = 0;
        // Slave进行消息分发后的位置
        private int dispatchPosition = 0;
        // 读取Slave和Master的socketChannel中的数据
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 读取Slave和Master的socketChannel中的数据备份，进行两个ByteBuffer的交换
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }
        // 是否是报告Slave的offset的时间，默认5秒报告一次offset
        private boolean isTimeToReportOffset() {
            // 距上次向master发送offset的时间
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            // slave向master发送offset的时间间隔默认5秒
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }
        // Slave向Master发送当前Slave的commitlog的最大offset
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            // flip()方法flip方法将Buffer从写模式切换到读模式。调用flip()方法会将position设回0，并将limit设置成之前position的值。并不会清除数据
            // position、limit方法也不会清除数据，只是修改指针，方便数据的读取
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            // 向socket写入最大的offset，最多写3次，socketChannel为非阻塞模式，不能保证一次写入数据到socketChannel成功，这里写三次；
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }
            // 本次发送offset的时间
            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            // 若超过3次还未能将8字节发送到Master Broker，说明存在问题，返回false，关闭此SocketChannel
            return !this.reportOffset.hasRemaining();
        }
        // ByteBuffer交换，创建新的空间
        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }
        //交换两个ByteBuffer
        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }
        // 处理Master返回的待处理消息
        private boolean processReadEvent() {
            // 读取数据的readSize的大小为0的次数，>=3次，不再处理这个读事件
            int readSizeZeroTimes = 0;
            // byteBufferRead 还可以空间，继续存放数据
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 从socketChannel中读取数据到byteBufferRead中
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    // 如果读取到数据
                    if (readSize > 0) {
                        // 重置readSizeZeroTimes为0
                        readSizeZeroTimes = 0;
                        // 处理Master返回的message消息可能包含多个message，单个消息逐条处理，并写入Slave的commitLog文件
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    // 读取数据为0的次数>=3次，不再处理这个读事件
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        // 处理失败
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }
        // 处理Master返回的message消息可能包含多个message，单个消息逐条处理，并写入Slave的commitLog文件
        private boolean dispatchReadRequest() {
            // 消息头：8字节消息的offset+4字节的消息的长度
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            // position为ByteBuffer的开始位置readSocketPos
            int readSocketPos = this.byteBufferRead.position();
            // 循环
            while (true) {
                // 这次读取到的新数据的大小
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                // diff>8,才能包含一条消息，才值得处理
                if (diff >= msgHeaderSize) {
                    // Master返回消息的的offset值
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    // 读取一条消息体的大小，然后读取这条消息，然后进行消息写入commitlog文件
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);
                    //获取当前本地的commitlog偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }
                    // 这次读取到的新数据的大小包含消息体，进行消息体处理
                    if (diff >= (msgHeaderSize + bodySize)) {
                        // 创建字节数组
                        byte[] bodyData = new byte[bodySize];
                        // 设置position位置，开始从byteBufferRead读取message的消息体的数据。
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        // 读取一个消息体的大小，读取一条消息
                        this.byteBufferRead.get(bodyData);
                        // Slave同步Master的一条消息数据，写入Slave的commitlog文件
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);
                        // 设置byteBufferRead的position位置回到读的位置
                        this.byteBufferRead.position(readSocketPos);
                        // 更新分发的位置
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }
                // 如果byteBufferRead没有空间存储数据，重新分配ByteBuffer
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }
        // 若slave的offset更新，需要再次发送新的slave的offset
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }
        // Salve连接Master
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                //masterAddress
                String addr = this.masterAddress.get();
                if (addr != null) {
                    // address:port转换为InetSocketAddress
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        // Slave连接Master的socket通道
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 将这个socket通过注册到selector
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                // 当前Slave最大的offset，表示slave当前向master已经发送了的offset
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }
            // 是否连接Master成功
            return this.socketChannel != null;
        }
        // 关闭和Master的连接
        private void closeMaster() {
            if (null != this.socketChannel) {
                try {
                    // selector取消
                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }
                    // socketChannel关闭
                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }
                // 清空数据、还原空间
                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 连接Master
                    if (this.connectMaster()) {
                        // slave是否向Master发送offset消息,默认5秒发送一次
                        if (this.isTimeToReportOffset()) {
                            //Slave向Master发送当前Slave的commitlog的最大offset
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            // 没有写完,返回false，未能将8字节offset发送到Master Broker，说明存在问题，返回false，关闭此SocketChannel
                            if (!result) {
                                this.closeMaster();
                            }
                        }
                        // selector使socketChannel等待1秒钟，等待
                        // I/O复用，检查是否有读事件
                        this.selector.select(1000);
                        // 处理Master返回的待处理消息，将返回的消息写入commitlog文件，并构建consumequeue、indexfile索引文件
                        // 如果处理失败表示，socketChannel存在问题，将此SocketChannel关闭
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            // 关闭Master
                            this.closeMaster();
                        }
                        // 处理完读事件后，若slave的offset更新，需要再次发送新的slave的offset
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        // 两次报告Slave offset超时，也会断开Slave与Master的socketChannel连接；
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        // 连接失败，等待5秒;并不涉及线程之间的wait和notify操作等
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    // 等待5秒，并不涉及线程之间的wait和notify操作等，然后再进行while循环，再次连接到master
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
