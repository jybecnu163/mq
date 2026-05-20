package com.minmq.broker.meta;

import com.minmq.common.protocol.BrokerInfo;
import com.minmq.common.protocol.MetaStore;
import com.minmq.common.protocol.PartitionInfo;

import java.util.Collections;
import java.util.List;

public class InMemoryMetaStore implements MetaStore {
    @Override
    public void registerBroker(BrokerInfo info) throws Exception {
// 单机模式无需操作
    }

    @Override
    public void unregisterBroker(String brokerId) throws Exception {
        // 无操作
    }

    @Override
    public List<BrokerInfo> getAliveBrokers() throws Exception {
        return Collections.emptyList(); // 单机模式，默认没有分区信息
    }

    @Override
    public List<PartitionInfo> getTopicPartitions(String topic) throws Exception {
        return Collections.emptyList(); // 单机模式，默认没有分区信息
    }

    @Override
    public void commitConsumerOffset(String group, String topic, int partition, long offset) throws Exception {
        // 单机模式仍使用本地文件进度，此接口可预留
    }

    @Override
    public long getConsumerOffset(String group, String topic, int partition) throws Exception {
        return -1; // 不支持
    }

    @Override
    public void close() {
        // nothing
    }
}
