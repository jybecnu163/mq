package com.mymq.broker.store;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Broker 级别的存储管理器，单例（整个 Broker 进程仅一个实例）
 * 负责管理 MessageLog、消费者索引等全局状态
 */
public class BrokerStore {
    private static final Logger log = LoggerFactory.getLogger(BrokerStore.class);

    private final MessageLog messageLog;
    // 所有消费者组索引管理器，key = "topic-group"
    private final Map<String, ConsumeIndexManager> groupIndexes = new ConcurrentHashMap<>();
    private final String dataDir;

    private final DelayMessageScheduler delayScheduler;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    // 当前分钟的临时累加器
    private final AtomicLong producedThisMinute = new AtomicLong();
    private final AtomicLong consumedThisMinute = new AtomicLong();

    // 过去 60 分钟的环形缓冲区
    private final AtomicLongArray productionRates = new AtomicLongArray(60);
    private final AtomicLongArray consumptionRates = new AtomicLongArray(60);

    // 环形缓冲区当前写入位置
    private volatile int minuteIndex = 0;

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
                    if (entry.getKey().startsWith(topic + "!@#$")) {
                        entry.getValue().appendOffset(offset, now);
                    }
                }
                log.info("Delay message expired and delivered: topic={}, body={}", topic, body);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        this.meterRegistry = meterRegistry;
        // 从磁盘恢复未到期的延时消息
        delayScheduler.recover();

        // 使用 Gauge 暴露活跃连接数
        Gauge.builder("mq_active_connections", activeConnections, AtomicInteger::get)
                .description("Current number of active client connections")
                .register(meterRegistry);

        // 启动定时清理任务：每小时执行一次，删除 3 天前且已消费的数据
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
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

        // 每分钟轮转一次
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            long prod = producedThisMinute.getAndSet(0);
            long cons = consumedThisMinute.getAndSet(0);
            productionRates.set(minuteIndex, prod);
            consumptionRates.set(minuteIndex, cons);
            minuteIndex = (minuteIndex + 1) % 60;
        }, 1, 1, TimeUnit.MINUTES);
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
            if (entry.getKey().startsWith(topic + "!@#$")) {
                entry.getValue().appendOffset(offset, now);
            }
        }
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
    public ConsumeIndexManager getOrCreateIndex(String topic, String group) {
        String key = topic + "!@#$" + group;
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

    public Set<String> getAllTopics() {
        Set<String> topics = new HashSet<>();
        // topic 信息可以从 groupIndexes 的 key 中提取，或者维护一个单独的 topic 集合
        for (String key : groupIndexes.keySet()) {
            String topic = key.substring(0, key.lastIndexOf("!@#$"));
            topics.add(topic);
        }
        return topics;
    }

    public Map<String, Set<String>> getConsumerGroups() {
        Map<String, Set<String>> topicGroups = new HashMap<>();
        for (String key : groupIndexes.keySet()) {
            int idx = key.lastIndexOf("!@#$");
            String topic = key.substring(0, idx);
            String group = key.substring(idx + 4);
            topicGroups.computeIfAbsent(topic, k -> new HashSet<>()).add(group);
        }
        return topicGroups;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public AtomicLongArray getConsumptionRates() {
        return consumptionRates;
    }

    public AtomicLongArray getProductionRates() {
        return productionRates;
    }

    public AtomicLong getConsumedThisMinute() {
        return consumedThisMinute;
    }

    public AtomicLong getProducedThisMinute() {
        return producedThisMinute;
    }

    /**
     * 返回过去 60 分钟的生产量数组，下标 0 是最新一分钟，59 是最远一分钟
     */
    public long[] getMinuteProductionRates() {
        return getLongs(productionRates);
    }

    public long[] getMinuteConsumptionRates() {
        return getLongs(consumptionRates);
    }

    private long[] getLongs(AtomicLongArray consumptionRates) {
        long[] rates = new long[60];
        for (int i = 0; i < 60; i++) {
            int idx = (minuteIndex - 1 - i + 60) % 60;
            rates[i] = consumptionRates.get(idx);
        }
        return rates;
    }

    public void close() {
        messageLog.close();
        for (ConsumeIndexManager idx : groupIndexes.values()) {
            try {
                idx.close();
            } catch (Exception e) {
                log.error("Failed to close ConsumeIndexManager", e);
            }
        }
        // 延时调度器如果有资源也可关闭
    }
}