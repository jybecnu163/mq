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

    private final DelayMessageScheduler delayScheduler;

    public BrokerStore(String dataDir) throws Exception {
        this.dataDir = dataDir;
        this.messageLog = new MessageLog(dataDir);

        // 初始化延时调度器，并定义到期后的处理：写入 messageLog 并追加索引
        this.delayScheduler = new DelayMessageScheduler(dataDir, (topic, body, tags) -> {
            try {
                long offset = messageLog.append(topic, body, tags);
                // 为所有已注册的消费者组追加索引
                for (Map.Entry<String, ConsumeIndexManager> entry : groupIndexes.entrySet()) {
                    if (entry.getKey().startsWith(topic + "-")) {
                        entry.getValue().appendOffset(offset);
                    }
                }
                System.out.println("Delay message expired and delivered: topic=" + topic + ", body=" + body);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        // 从磁盘恢复未到期的延时消息
        delayScheduler.recover();
    }

    /**
     * 新增延时消息入口
     */
    public void scheduleDelayMessage(long expireTime, String topic, String body, String tags) throws Exception {
        delayScheduler.schedule(expireTime, topic, body, tags);
    }

    /**
     * 追加消息，返回物理偏移量
     */
    public long appendMessage(String topic, String body, String tags) throws Exception {
        return messageLog.append(topic, body, tags);
    }

    // 原有 readMessage 删除，或改为调用 readMessageData().getBody()
    public String readMessage(long offset) throws Exception {
        return readMessageData(offset).body;
    }

    /**
     * 根据物理偏移量读取消息体
     */
    public MessageLog.MessageEntry readMessageData(long offset) throws Exception {
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