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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 *  MappedFile 的存储集合和管理器，是对 ${ROCKET_HOME}/store/commitlog 文件夹的封装
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    // 批量删除文件大小
    private static final int DELETE_FILES_BATCH_MAX = 10;
    // 存储路径${ROCKET_HOME}/store/commitlog,该目录下会存在多个内存映射文件
    private final String storePath;
    // 单个文件的存储大小
    private final int mappedFileSize;
    // mappedFile 文件集合
    // 一个线程安全的 ArrayList 的变种，通过可 reentrantLock 可重入锁实现数组的新建和数组旧有内容的 copy 到新建的数组，然后返回新建的数组
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    // 创建 MappedFile 服务类
    private final AllocateMappedFileService allocateMappedFileService;
    // 当前刷盘指针，表示该指针之前的所有数据全部持久化到磁盘
    // MappedFile 中的 MappedByteBuffer 中数据写入磁盘的指针，该指针之前的所有数据全部持久化到磁盘
    private long flushedWhere = 0;

    // Java 应用程序态数据要写入nio内存映射的ByteBuffer的提交了位置的指针
    // commitWhere 只有开启 transientStorePool 的前提下才有作用；
    // commitWhere 代表着 transientStorePool 中直接内存 ByteBuffer 需要提交数据到 MappedByteBuffer 直接内存的，位置为已经提交了数据的位置。下次要提交的开始位置，上次提交的结尾位置。
    private long committedWhere = 0;
    // 存储时间
    private volatile long storeTimestamp = 0;

    /**
     * @param storePath 存储路径
     * @param mappedFileSize mappedFile 文件大小
     * @param allocateMappedFileService mappedFile 文件创建服务
     */
    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 检测 mappedFile 起始和结束位置的差值是否等于 mappedFile 文件大小，来确定 mappedFile 文件是否被破坏。
     */
    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 根据消息存储时间戳来查找 MappedFile，从 MappedFile 列表中第一个文件开始查找，找到第一个最后一次更新时间大于待查找时间戳的文件，如果不存在，则返回最后一个 MappedFile 文件。
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * 获取 commitlog 文件夹下还存在的文件，小于 reservedMappedFiles 所要求的个数返回 null
     * @param reservedMappedFiles 保留的文件个数
     * @return
     */
    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;
        // 如果文件夹下的文件，不满足给定的文件个数，直接返回 null
        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 删除这个 offset 之后的所有 mappedFile 文件，并修改当前 offset 所在 mappedFile 文件的 wrotePosition、committedPosition、flushedPosition 指针位置
     * @param offset
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            // 每个 MappedFile 文件的最后 offset 值
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 文件最后 offset 值大于给定 offset，则进行文件删除
            if (fileTailOffset > offset) {
                // 获取给定 offset 所在的 mappedFile 文件，并修改当前 offset 所在 mappedFile 文件的 wrotePosition、committedPosition、flushedPosition 指针位置
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    // 删除 mappedFile 对应的物理文件
                    file.destroy(1000);
                    // 加入被删除文件集合
                    willRemoveFiles.add(file);
                }
            }
        }
        // 删除文件具体方法
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 删除过期文件
     * @param files 需要删除的文件
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 项目启动，加载 commitlog 文件夹下对应的文件，只是创建了对应的MappedFile，并没有从磁盘加载文件
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 根据文件名（offset）排序
            Arrays.sort(files);
            for (File file : files) {
                // 如果物理文件大小 ！= mappedFileSize，说明文件被破坏了，直接返回false
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    // 更新 mappedFile 文件指针
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    // 加入映射文件集合
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取最后存储消息的映射mappedFile
     *
     * @param startOffset mappedFile 开始写文件的offset
     * @param needCreate 是否需要创建新的 mappedFile 文件
     * @return
     */
    //
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        // 创建映射文件的起始偏移量,也就是即将的mappedfile文件名称
        long createOffset = -1;
        // 获取最后一个映射文件，如果为null或者写满则会执行创建逻辑
        MappedFile mappedFileLast = getLastMappedFile();
        // mappedFileLast == null，表示需要创建新的 mappedFile 文件，创建新文件的offset值；
        if (mappedFileLast == null) {
            // 计算将要创建的映射文件的起始偏移量
            // 如果startOffset<=mappedFileSize则起始偏移量为0
            // 如果startOffset>mappedFileSize则起始偏移量为是mappedFileSize的倍数
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        // 映射文件满了，创建新的映射文件
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            // 创建的映射文件的偏移量等于最后一个映射文件的起始偏移量  + 映射文件的大小（commitlog文件大小）
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        // 创建新的映射文件
        if (createOffset != -1 && needCreate) {
            // 构造commitlog 文件名称
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            // nextNextFilePath，预先创建下一个 mappedFile 文件，通过 allocateMappedFileService 服务，一起创建两个文件，预先创建下一个文件
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;
            // 优先通过allocateMappedFileService中方式构建映射文件，预分配方式，性能高
            // 如果上述方式失败则通过new创建映射文件
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                // 是否是 MappedFileQueue 队列中第一个文件
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    /**
     * 获取最后存储消息的映射mappedFile
     * @param startOffset mappedFile 开始写文件的offset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个 mappedFile 文件
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    /**
     * 重置 offset
     * @param offset
     * @return
     */
    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            // 最后写入文件的最后 offset 值
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            // 重置 offset 值 和 最后写入 offset 值的差值
            long diff = lastOffset - offset;
            // 最大差值
            final int maxDiff = this.mappedFileSize * 2;
            // 跨越两个文件，不能重置 offset 值；
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();
        // 大于这重置 offset 所在mappedFile 文件进行集合中的删除
        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * @return 获取存储文件中的最小偏移量，并不是直接返回0，而是返回MappedFile 的getFileFromOffset()
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * @return 获取最大文件的偏移量，最后一个文件的开始offset + mappedFile 可读文件的开始位置
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * @return 可写文件的最大开始位置
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 多少数据需要提交到映射内存 ByteBuffer 中
     * @return
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 多少数据需要刷盘
     * @return
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    /**
     * 删除最后一个 mappedFile
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 根据 offset 删除过期文件
     * @param offset
     * @param unitSize
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 刷新到内存映射文件 ByteBuffer
     * @param flushLeastPages 需要刷新的最近的一页
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 根据 offset 定位到 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交数据到 内存映射 byteBuffer 中
     * @param commitLeastPages
     * @return
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 获取提交的 mappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            // 已经提交了的 offset
            int offset = mappedFile.commit(commitLeastPages);
            // 下次提交的 offset 值
            long where = mappedFile.getFileFromOffset() + offset;
            // 提交了的值和下次要提交的值进行对比，看是否提交成功，提交成功，返回 true，否则返回失败
            result = where == this.committedWhere;
            //  下次提交的 offset 值
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 根据 offset 定位 MappedFile 的算法为 （int)((offset/this.mappedFileSize) - (mappedFile.getFileFromOffset()/this.MappedFileSize))，获取这个 MappedFile 在 mappedFiles 的下标，然后获取 MappedFile 文件。
     * RocketMQ commitlog 日志文件有定时删除功能，所以 commitlog 文件夹下的文件个数是会发生改变的，所以下标的起始位置也会发生改变，动态确定 offset 所在文件的下标为:总文件的个数 - 现有文件个数 = 这个 offset 所在 MappedFile 文件集合中的下标值
     *
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    // mappedFile 文件下标
                    // (offset / this.mappedFileSize) 为这个 offset 所在 mappedFile 文件中的第几个个数，定义为：sum
                    //  (firstMappedFile.getFileFromOffset() / this.mappedFileSize)) 为第一个文件所在的文件个数， 定义为：first
                    // sum - first 为这个 offset，在现有的 mappedFiles 集合文件的下标。
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    /**
     * 获取 mappedFiles 集合中第一个文件
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * consumequeue根据消息的物理offset查找mappedFile
     * @param offset consumequeue的物理offset
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
