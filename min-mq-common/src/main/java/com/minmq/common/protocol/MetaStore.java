package com.minmq.common.protocol;

import java.util.List;

public interface MetaStore {
    /**
     * 注册 Broker 节点
     */
    void registerBroker(BrokerInfo info) throws Exception;

    /**
     * 注销 Broker 节点
     */
    void unregisterBroker(String brokerId) throws Exception;

    /**
     * 获取所有存活的 Broker 列表
     */
    List<BrokerInfo> getAliveBrokers() throws Exception;

    /**
     * 获取指定 Topic 的分区信息
     */
    List<PartitionInfo> getTopicPartitions(String topic) throws Exception;

    /**
     * 提交消费者组的消费偏移量
     */
    void commitConsumerOffset(String group, String topic, int partition, long offset) throws Exception;

    /**
     * 获取消费者组的消费偏移量
     */
    long getConsumerOffset(String group, String topic, int partition) throws Exception;

    /**
     * 关闭连接
     */
    void close();
}
