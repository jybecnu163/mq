package com.minmq.broker.store;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minmq.common.protocol.BodyCodec;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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
    private final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
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
                    if (entry.getKey().startsWith(topic + "-")) {
                        entry.getValue().appendOffset(offset, now);
                    }
                }
                log.info("Delay message expired and delivered: topic={}, body={}", topic,
                        new String(body, StandardCharsets.UTF_8));

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
    public void scheduleDelayMessage(long expireTime, String topic, byte[] body, String tags) throws Exception {
        delayScheduler.schedule(expireTime, topic, body, tags);
    }

    /**
     * 追加消息，返回物理偏移量
     */
    public long appendMessage(String topic, byte[] body, String tags) throws Exception {
        // 保留旧重载，内部调用新增的带二进制参数的方法
        return appendMessage(topic, body, tags, BodyCodec.TEXT);
    }

    public long appendMessage(String topic, byte[] body, String tags,
                              BodyCodec bodyCodec) throws Exception {
        long now = System.currentTimeMillis();
        long offset = messageLog.append(topic, body, tags, now, bodyCodec);
        // 为所有已注册的消费者组追加索引
        for (Map.Entry<String, ConsumeIndexManager> entry : groupIndexes.entrySet()) {
            if (entry.getKey().startsWith(topic + "-")) {
                entry.getValue().appendOffset(offset, now);
            }
        }
        return offset;
    }

    // 原有 readMessage 删除，或改为调用 readMessageData().getBody()
    public byte[] readMessage(long offset) throws Exception {
        return readMessageData(offset).getB();
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
        ConsumeIndexManager indexMgr = groupIndexes.get(key);
        if (indexMgr == null) {
            synchronized (groupIndexes) {
                indexMgr = groupIndexes.get(key);
                if (indexMgr == null) {
                    indexMgr = new ConsumeIndexManager(dataDir, topic, group);
                    groupIndexes.put(key, indexMgr);

                    // 回填历史消息索引
//                    rebuildIndexFromHistory(topic, indexMgr);
                }
            }
        }
        return indexMgr;
    }

    /**
     * 扫描 message.log 中所有指定 topic 的消息，追加到该消费者的索引中
     */
    private void rebuildIndexFromHistory(String topic, ConsumeIndexManager indexMgr) throws Exception {
        // 从头扫描所有段文件（这个操作比较重，仅用于演示）
        long scannedOffset = 0; // 全局偏移量起点从0开始，实际应从第一个段开始
        // 实际上我们需要从 messageLog 的第一个段开始，按顺序遍历所有消息
        // MessageLog 需要提供遍历接口。此处简化：直接读取消息并检查 topic。
        // 注意：这只是原型方案，生产环境中应该维护全局 topic 索引。
        long startOffset = 0;
        long endOffset = 99999;//messageLog.getTotalSize(); // 需新增方法获取总大小
        long current = startOffset;
        while (current < endOffset) {
            try {
                MessageLog.MessageEntry entry = messageLog.readMessage(current);
                if (topic.equals(entry.getP())) {
                    indexMgr.appendOffset(current, entry.getTs());
                }
                // 计算本条消息的长度（4字节长度头 + 数据长度）
                byte[] data = mapper.writeValueAsBytes(entry); // 需序列化一次计算长度，性能低
                current += 4 + data.length;
            } catch (Exception e) {
                // 可能遇到文件末尾损坏，停止
                break;
            }
        }
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
            String topic = key.substring(0, key.lastIndexOf("-"));
            topics.add(topic);
        }
        return topics;
    }

    public Map<String, Set<String>> getConsumerGroups() {
        Map<String, Set<String>> topicGroups = new HashMap<>();
        for (String key : groupIndexes.keySet()) {
            int idx = key.lastIndexOf("1");
            String topic = key.substring(0, idx);
            String group = key.substring(idx + 1);
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