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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * MappedFile 是 RocketMQ 内存映射文件的具体实现
 * https://my.oschina.net/mingxungu/blog/3083999
 * https://tinylcy.me/2019/the-design-of-rocketmq-message-storage-system/
 * https://juejin.cn/post/6844903862147497998
 * https://www.cnblogs.com/duanxz/p/5020398.html
 * http://wuwenliang.net/2020/02/11/%E8%B7%9F%E6%88%91%E5%AD%A6RocketMQ%E4%B9%8B%E6%B6%88%E6%81%AF%E6%8C%81%E4%B9%85%E5%8C%96%E5%8E%9F%E7%90%86%E4%B8%8EMmap/
 *
 */
public class MappedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 当前JVM实例中 MappedFile 虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    // 当前JVM实例中MappedFile对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 即将写入消息的mappedFile 的位置
    // 当前 MappedFile 文件的写指针，从 0 开始(内存映射文件的写指针)
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 当前文件的提交到 MappedBuffer的指针，如果开启 transientStorePoolEnable，则数据会存储在 TransientStorePool 中，然后提交到内存映射 ByteBuffer 中,再刷写到磁盘
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 刷写到磁盘指针，该指针之前的数据持久化到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 文件大小
    protected int fileSize;
    // 文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    // 堆外内存 ByteBuffer，如果不为空，数据首先将存储在该 Buffer 中，然后提交到 MappedFile 对应的内存映射文件 Buffer。
    // transientStorePoolEnable 为true时不为空。
    protected ByteBuffer writeBuffer = null;
    // 堆内存池，transientStorePoolEnable 为true 时启用
    protected TransientStorePool transientStorePool = null;
    // 文件名称
    private String fileName;
    // mappedFile 文件的开始偏移量地址
    private long fileFromOffset;
    // 物理文件
    private File file;
    // 物理文件对应的内存映射Buffer
    private MappedByteBuffer mappedByteBuffer;
    // 文件最后一次内容写入的时间
    private volatile long storeTimestamp = 0;
    // 是否是 MappedFileQueue 队列中第一个文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * @param fileName 物理文件路径
     * @param fileSize 文件大小
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    /**
     * 确保文件路径存在，不存在，进行路径文件创建
     * @param dirName
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * MappedFile 初始化
     * @param fileName
     * @param fileSize
     * @param transientStorePool 临时内存存储池，里面包装了堆外内存（直接内存）
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 如果 transientStorePoolEnable 为true，则初始化 MappedFile 的 WriterBuffer
        // 从 transientStorePool 借出一个直接内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * MappedFile 初始化,并做好 mappedFile 和 mappedByteBuffer 的NIO 直接内存映射关系
     *
     * @param fileName 物理文件路径
     * @param fileSize mappedFileSize 文件大小
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        // 物理文件名称
        this.file = new File(fileName);
        // 文件开始位置
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 确保文件路径存在，不存在，进行路径文件创建
        ensureDirOK(this.file.getParent());

        try {
            // 通过 RandomAccessFile 创建读写文件通道，并将文件内容使用NIO 的内存映射 Buffer 将文件映射到内存中
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 物理文件对应的内存映射Buffer
            // 通过 NIO 文件通道和mappedFileSize 大小，创建内存映射文件 mappedByteBuffer
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 当前JVM实例中 MappedFile 虚拟内存
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            // 当前JVM实例中MappedFile对象个数
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }
    // 获取最后一次修改时间
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }
    // 追加消息到 mappedFile 文件中
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }
    // 追加消息到 mappedFile 文件中
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        // 当前消息写入的位置
        int currentPos = this.wrotePosition.get();
        // 如果位置超过文件大小，抛出 UNKNOWN_ERROR 未知错误异常；
        // 小于文件大小，进行消息存储
        if (currentPos < this.fileSize) {
            // slice 薄片，创建一个与 MappedFile 的共享内存区，并设置 position 当前指针。
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新消息队列，逻辑偏移量
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }
    // 获取文件开始 offset
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }
    // ConsumeQueue的索引条目存储到自己的MappedFile问价
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            //更新写入的位置
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘指的是将内存中的数据刷写到磁盘，永久存储在磁盘中
     * 刷写磁盘，直接调用 mappedByteBuffer 或 fileChannel 的force 方法，将内存中的数据持久化到磁盘，那么 flushedPosition 应等于上一次 commit指针，
     * 因为上一次提交的数据就是进入到 MappedByteBuffer 中的数据；如果 writeBuffer 为空，数据是直接进入到 MappedByteBuffer,wrotePosition 代表的是
     * MappedByteBuffer 中的指针，故设置 flushedPosition 为 writePosition。
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 提交操作是指:将 transientStorePool 中直接内存映射 ByteBuffer 的数据，通过 NIO channel 将数据，提交到直接内存映射的 MappedByteBuffer 中，然后再进行刷盘操作，将数据落入磁盘。
     * 如果没有使用 transientStorePool，则不存在 commit 过程。直接将数据映射到了 MappedByteBuffer 中，然后进行刷盘操作。所以说 commit 操作是对使用了 transientStorePool 中的临时直接内存 ByteBuffer 来说的。
     *
     * commitLeastPages 为本次提交最小的页数，如果待提交数据不满 commitLeastPages，则不执行提交操作，待下次提交。
     * writerBuffer 如果为空，直接返回 wrotePosition 指针，无须执行 commit 操作，表明 commit 操作主题是 writerBuffer
     * @param commitLeastPages 为本次提交最小的页数，默认为 4
     * @return 返回已经提交了的截止位置
     */
    public int commit(final int commitLeastPages) {
        // writerBuffer 为 null，表示 transientStorePool 未开启，不存在从堆外内存 writerBuffer，向映射内存提交动作，表明提交内存的主题时 writerBuffer；
        // writerBuffer 为 null，可能就不需要 commit 这个操作了，只需要返回wrotePosition作为committedPosition
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // 判断是否执行 commit 操作
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // 具体提交实现
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // 所有的脏数据已经提交到了 FileChannel，释放writerBuffer
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        // 返回已经提交了的截止位置
        return this.committedPosition.get();
    }

    /**
     * 具体提交实现
     * 首先创建writeBuffer的共享缓存区，然后将新创建的 position 回退到上一次提交的位置（committedPosition），
     * 设置 limit 为writePosition(当前最大有效数据指针)，然后把 committedPosition 到 wrotePosition 的数据复制（写入）到 FileChannel 中，然后更新 committedPosition 指针为 wrotePosition，
     * commit 的作用就是将 MappedFile#writeBuffer 中的数据提交到文件通道 fileChannel 中。
     *
     * ByteBuffer 使用技巧：slice() 方法创建一个共享缓存区， 与原先的 ByteBuffer 共享内存，但维护一套独立的指针 position mark limit
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        // 写指针
        int writePos = this.wrotePosition.get();
        // 上次提交的指针
        int lastCommittedPosition = this.committedPosition.get();
        // 写指针 > 上次提交的指针
        if (writePos - this.committedPosition.get() > 0) {
            try {
                // writeBuffer 一定不为 null，因为这个方法调用的地方已经进行了判断，如果writerBuffer 为 null，直接返回了一个wrotePosition;
                // writerBuffer 不为 null,一定使用了 transientStorePool，将 transientStorePoll 中开辟直接内存的 ByteBuffer 数据进行提交到，MappedByteBuffer 直接内存映射中。
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 这次提交的开始位置
                byteBuffer.position(lastCommittedPosition);
                // 设置byteBuffer大小，这次取出需要提交的截止位置
                byteBuffer.limit(writePos);
                // 设置提交到fileChannel的开始位置
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                // 设置writePos为已经提交了的位置
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }
    // 是否可以刷盘
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否执行提交操作
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();
        // 如果文件已满，返回true
        if (this.isFull()) {
            return true;
        }
        // 如果 commitLeastPages > 0,则比较 wrotePosition(当前writeBuffer的写指针)与上一次提交的指针(committedPostion)的差值，除以 OS_PAGE_SIZE 得到当前数据页的数量，
        // 如果大于 commitLeastPages 则返回 true，如果 commitLeastPages 小于0，表示只要存在数据页就提交。
        // 如果提交页数 > 0
        // 判断写的页数 - 上次提交的页数，是否大于要提交的页数
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        // 如果 commitLeastPages <=0,表示只要存在数据页就提交
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 根据pos在mappedBuffer中的位置，还有获取多少个字节，来获取 mappedBuffer 中的数据
     * @param pos
     * @param size
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 获取 mappedBuffer 中的数据
     * @param pos mappedBuffer 中的一个位置，必须小于可读数据的位置
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }
    // 清除mappedFile的mappedByteBuffer，减少内存空间统计
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 文件的销毁，资源的释放
     *
     * @param intervalForcibly 拒绝被销毁的最大存活时间
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);
        // mappedFile文件是否被清除
        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 可读文件的开始位置，有效数据的最大位置
     * RocketMQ 文件的一个组织方式是内存映射文件，预先申请一块连续的固定大小的内存，需要一套指针标识当前最大有效数据的位置，getReadPosition 是获取最大有效数据偏移量的方法。
     * 如果 transientStorePool 启用，其中的 writerBuffer 不为 null；位置为：writerBuffer 的提交位置，未提交到 MappedByteBuffer 的数据不可读取；否则为 mappedByteBuffer 中的写入位置。
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 对 mappedFile 文件进行预热，将内存和磁盘映射起来，然后每页写入占位数据0，然后将这些0数据，刷新到磁盘，进行磁盘预热。
     *
     * 当调用Mmap进行内存映射后，OS只是建立了虚拟内存地址至物理地址的映射表，而实际并没有加载任何文件至内存中。
     * 程序要访问数据时，OS会检查该部分的分页是否已经在内存中，如果不在，则发出一次 缺页中断。X86的Linux中一个标准页面大小是4KB，
     * 那么1G的CommitLog需要发生 1024KB/4KB=256次 缺页中断，才能使得对应的数据完全加载至物理内存中。
     *
     *
     * 为什么每个页都需要写入数据呢？
     *
     * RocketMQ在创建并分配MappedFile的过程中预先写入了一些随机值到Mmap映射出的内存空间里。原因在于：
     * 仅分配内存并进行mlock系统调用后并不会为程序完全锁定这些分配的内存，原因在于其中的分页可能是写时复制的。因此，就有必要对每个内存页面中写入一个假的值。
     * 锁定的内存可能是写时复制的，这个时候，这个内存空间可能会改变。这个时候，写入假的临时值，这样就可以针对每一个内存分页的写入操做会强制 Linux 为当前进程分配一个独立、私有的内存页。
     *
     * 写时复制
     * 写时复制：子进程依赖使用父进程开创的物理空间。
     * 内核只为新生成的子进程创建虚拟空间结构，它们来复制于父进程的虚拟究竟结构，但是不为这些段分配物理内存，它们共享父进程的物理空间，当父子进程中有更改相应段的行为发生时，再为子进程相应的段分配物理空间。
     * https:www.cnblogs.com/biyeymyhjob/archive/2012/07/20/2601655.html
     *
     * 为了避免OS检查分页是否在内存中的过程出现大量缺页中断，RocketMQ在做Mmap内存映射的同时进行了madvise系统调用，
     * 目的是使OS做一次内存映射后，使对应的文件数据尽可能多的预加载至内存中，降低缺页中断次数，从而达到内存预热的效果。
     * RocketMQ通过map+madvise映射后预热机制，将磁盘中的数据尽可能多的加载到PageCache中，保证后续对ConsumeQueue和CommitLog的读取过程中，能够尽可能从内存中读取数据，提升读写性能。
     *
     * mappedFile 文件进行
     * @param type 刷盘类型
     * @param pages 4k，系统缓存页，刷盘大小： 1024 / 4 * 16
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        // 创建一个新的字节缓冲区，其内容是此缓冲区内容的共享子序列
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        // warmMappedFile 每间隔 OS_PAGE_SIZE 向 mappedByteBuffer 写入一个 0，此时对应页恰好产生一个缺页中断，操作系统为对应页分配物理内存
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // 刷盘方式是同步策略时，进行刷盘操作
            // 每修改 pages 个分页刷一次盘，相当于 4096 * 4k = 16M，每 16 M刷一次盘，1G 文件 1024M/16M = 64 次
            // force flush when flush disk type is sync
            // 如果刷盘策略为同步刷盘，需要对每个页进行刷盘
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // 防止垃圾回收
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        // 前面对每个页，写入了数据（0 占位用，防止被内存交互），进行了刷盘，然后这个操作是对所有的内存进行刷盘。
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            // 刷盘，强制将此缓冲区内容的任何更改写入包含映射文件的存储设备
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);
        //通过 JNA 调用 mlock 方法锁定 mappedByteBuffer 对应的物理内存，阻止操作系统将相关的内存页调度到交换空间（swap space），以此提升后续在访问 MappedFile 时的读写性能。
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }
    // 通过 JNA 调用 mlock 方法锁定 mappedByteBuffer 对应的物理内存，阻止操作系统将相关的内存页调度到交换空间（swap space），以此提升后续在访问 MappedFile 时的读写性能。
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        // RocketMQ的做法是，在做Mmap内存映射的同时进行madvise系统调用，目的是使OS做一次内存映射后对应的文件数据尽可能多的预加载至内存中，从而达到内存预热的效果。
        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    /**
     * 解锁 mappedFile 的内存锁定
     */
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
