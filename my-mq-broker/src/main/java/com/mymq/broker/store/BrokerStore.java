package com.mymq.broker.store;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Broker 级别的存储管理器，单例（整个 Broker 进程仅一个实例）
 * 负责管理 MessageLog、消费者索引等全局状态
 */
public class BrokerStore {
    private static final Logger log = LoggerFactory.getLogger(BrokerStore.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final MessageLog messageLog;
    // 所有消费者组索引管理器，key = "topic-group"
    private final Map<String, ConsumeIndexManager> groupIndexes = new ConcurrentHashMap<>();
    private final String dataDir;

    private final DelayMessageScheduler delayScheduler;

    // 指标
    private final Counter messagesProduced;
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    public BrokerStore(String dataDir, MeterRegistry meterRegistry) throws Exception {
        this.dataDir = dataDir;
        this.messageLog = new MessageLog(dataDir);

        // 初始化延时调度器，并定义到期后的处理：写入 messageLog 并追加索引
        this.delayScheduler = new DelayMessageScheduler(dataDir, (topic, body, tags) -> {
            try {
                long now = System.currentTimeMillis();
                long offset = messageLog.append(topic, body, tags);
                // 为所有已注册的消费者组追加索引
                for (Map.Entry<String, ConsumeIndexManager> entry : groupIndexes.entrySet()) {
                    if (entry.getKey().startsWith(topic + "-")) {
                        entry.getValue().appendOffset(offset, now);
                    }
                }
                log.info("Delay message expired and delivered: topic={}, body={}", topic, body);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        // 从磁盘恢复未到期的延时消息
        delayScheduler.recover();

        // 注册指标
        this.messagesProduced = Counter.builder("mq_messages_produced_total")
                .description("Total number of messages produced")
                .register(meterRegistry);

        // 使用 Gauge 暴露活跃连接数
        Gauge.builder("mq_active_connections", activeConnections, AtomicInteger::get)
                .description("Current number of active client connections")
                .register(meterRegistry);

        // 启动定时清理任务：每小时执行一次，删除 3 天前且已消费的数据
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long minOffset = getMinConsumerOffset();
                long threeDaysMs = 3L * 24 * 60 * 60 * 1000;
                int deleted = getMessageLog().deleteOldSegments(minOffset, threeDaysMs);
                if (deleted > 0) {
                    log.info("Cleanup deleted {} segments", deleted);
                }
            } catch (Exception e) {
                log.error("Segment cleanup failed", e);
            }
        }, 1, 1, TimeUnit.HOURS);
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
        long now = System.currentTimeMillis();
        long offset = messageLog.append(topic, body, tags, now);
        // 为所有已注册组追加索引（带时间戳）
        for (Map.Entry<String, ConsumeIndexManager> entry : groupIndexes.entrySet()) {
            if (entry.getKey().startsWith(topic + "-")) {
                entry.getValue().appendOffset(offset, now);
            }
        }
        messagesProduced.increment();   // 生产计数+1
        return offset;
    }

    // 原有 readMessage 删除，或改为调用 readMessageData().getBody()
    public String readMessage(long offset) throws Exception {
        return readMessageData(offset).getBody();
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

    public void onClientConnected() {
        activeConnections.incrementAndGet();
    }

    public void onClientDisconnected() {
        activeConnections.decrementAndGet();
    }

    // 定时清理（可在 BrokerServer 调度）
    public int cleanupOldSegments(long maxAgeMillis) {
        return messageLog.deleteOldSegments(getMinConsumerOffset(), maxAgeMillis);
    }

    public long getMinConsumerOffset() {
        long min = Long.MAX_VALUE;
        for (ConsumeIndexManager idx : groupIndexes.values()) {
            min = Math.min(min, idx.getConsumerOffset());
        }
        return min == Long.MAX_VALUE ? 0 : min;
    }

    public MessageLog getMessageLog() {
        return messageLog;
    }
}