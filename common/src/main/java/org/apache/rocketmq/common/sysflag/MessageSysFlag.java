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
package org.apache.rocketmq.common.sysflag;

/**
 * 消息系统标志位
 */
public class MessageSysFlag {
    // 压缩标志
    public final static int COMPRESSED_FLAG = 0x1;
    // 多个消息标签
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;
    // 非事务类型
    public final static int TRANSACTION_NOT_TYPE = 0;
    // 事务准备阶段类型
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    // 事务提交类型
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    // 事务回滚类型
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;
    // 消息出生地 ipv6 flag
    public final static int BORNHOST_V6_FLAG = 0x1 << 4;
    // 消息存储地 ipv6 flag
    public final static int STOREHOSTADDRESS_V6_FLAG = 0x1 << 5;
    // 事务类型
    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }
    // 重置事务类型
    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }
    // 消息压缩
    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }

}
