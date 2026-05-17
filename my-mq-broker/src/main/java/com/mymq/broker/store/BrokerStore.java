package com.mymq.broker.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker 级别的存储管理器，单例（整个 Broker 进程仅一个实例）
 * 负责管理 MessageLog、消费者索引等全局状态
 */
public class BrokerStore {
    private final MessageLog messageLog;
    // 所有消费者组索引管理器，key = "topic-group"
    private final Map<String, ConsumeIndexManager> groupIndexes = new ConcurrentHashMap<>();
    private final String dataDir;

    public BrokerStore(String dataDir) throws Exception {
        this.dataDir = dataDir;
        this.messageLog = new MessageLog(dataDir);
    }

    /**
     * 追加消息，返回物理偏移量
     */
    public long appendMessage(String topic, String body) throws Exception {
        return messageLog.append(topic, body);
    }

    /**
     * 根据物理偏移量读取消息体
     */
    public String readMessage(long offset) throws Exception {
        return messageLog.readMessage(offset);
    }

    /**
     * 获取或创建指定 topic + group 的消费索引管理器，并自动注册到 topicGroups
     */
    public ConsumeIndexManager getOrCreateIndex(String topic, String group) throws Exception {
        String key = topic + "-" + group;
        return groupIndexes.computeIfAbsent(key, k -> {
            try {
                return new ConsumeIndexManager(dataDir, topic, group);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 返回所有已注册的组索引管理器映射的只读视图，供 SEND 时遍历
     */
    public Map<String, ConsumeIndexManager> getAllGroupIndexes() {
        return groupIndexes;
    }
}