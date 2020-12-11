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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 额外服务的线程，比如 AllocateMappedFileService 提前创建 MappedFile 文件的服务
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;
    // 封装的线程
    private Thread thread;
    // 同步等待基数点
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    // 是否通知线程标志
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    // 线程是否停止
    protected volatile boolean stopped = false;
    // 是否是守护线程
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     * 开启额外服务线程
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    /**
     * 关闭额外服务
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭额外服务
     * @param interrupt 线程是否可中断
     */
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                // 线程自己中断自己，不能被其他线程中断，否则会抛出错误
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                // 这个线程等待 90 秒自动死亡
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }
    // 唤醒处理刷盘请求写磁盘线程，处理刷盘请求线程和提交刷盘请求之前的协调，通过控制hasNotified状态来实现写队列和读队列的交换
    public void wakeup() {
        // hasNotified默认值是false，未被唤醒，这个操作之后唤醒了，处理刷盘请求
        if (hasNotified.compareAndSet(false, true)) {
            // waitPoint默认是1，然后其他线程处理
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 设置hasNotified为false，未被通知，然后交换写对队列和读队列，重置waitPoint为（1），休息200ms，finally设置hasNotified为未被通知，交换写对队列和读队列
     * @param interval 200ms
     */
    protected void waitForRunning(long interval) {
        // compareAndSet(except,update);如果真实值value==except，设置value值为update，返回true；如果真实值value !=except，真实值不变，返回false；
        // 如果hasNotified真实值为true，那么设置真实值为false，返回true；hasNotified真实值为false，那就返回false，真实值不变
        // 如果已经通知了，那就状态变为未通知，如果是同步刷盘任务，交换写请求队列和读请求队列
        if (hasNotified.compareAndSet(true, false)) {
            // 同步刷盘：写队列和读队列交换
            this.onWaitEnd();
            return;
        }
        // 重置countDownLatch对象，等待接受刷盘请求的线程写入请求到requestsRead，写完后，waitPoint.countDown,唤醒处理刷盘请求的线程，开始刷盘
        //entry to wait
        waitPoint.reset();

        try {
            // 等待interval毫秒
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 设置是否通知为false
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }
    // 等待这个方法的步骤完成。比如：同步刷盘：写队列和读队列交换
    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
