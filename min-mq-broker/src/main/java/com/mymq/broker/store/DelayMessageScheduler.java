package com.minmq.broker.store;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 两级时间轮调度器：
 * 1. 从磁盘加载延时消息到内存时间轮
 * 2. 运行时接收新延时消息，先落盘再入内存时间轮
 * 到期后回调指定的消息投递逻辑（写入 MessageLog 并通知消费者）
 */
public class DelayMessageScheduler {
    private final DelayMessageStore diskStore;
    private final HashedWheelTimer timer;

    // 回调：当消息到期时调用，参数为 (topic, body)
    public interface DelayedMessageHandler {
        void onExpire(String topic, String body, String tags);
    }

    private final DelayedMessageHandler handler;

    public DelayMessageScheduler(String dataDir, DelayedMessageHandler handler) {
        this.diskStore = new DelayMessageStore(dataDir);
        this.handler = handler;
        // 创建内存时间轮，100ms 刻度，每轮 512 个槽
        this.timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 512);
        timer.start();
    }

    /**
     * 系统启动时调用，从磁盘加载所有未到期的延时消息
     */
    public void recover() throws IOException {
        List<String> allFiles = diskStore.listAllHourFiles();
        long now = System.currentTimeMillis();
        for (String fileName : allFiles) {
            long hourTimestamp = parseHourFromFileName(fileName);
            if (hourTimestamp >= now / 3600000 * 3600000) { // 该小时尚未过去
                List<DelayMessageStore.DelayMessageEntry> entries = diskStore.loadHourFile(fileName);
                for (DelayMessageStore.DelayMessageEntry entry : entries) {
                    if (entry.expireTime > now) {
                        scheduleMemoryTask(entry.expireTime, entry.topic, entry.body, entry.tags);
                    } else {
                        // 已经过期（虽然文件名表示的小时未过去，但精确时间可能已过），立即投递
                        handler.onExpire(entry.topic, entry.body, entry.tags);
                    }
                }
            }
        }
    }

    /**
     * 新增一条延时消息：先持久化到磁盘，再放入内存时间轮
     */
    public void schedule(long expireTime, String topic, String body, String tags) throws IOException {
        diskStore.store(expireTime, topic, body, tags);
        scheduleMemoryTask(expireTime, topic, body, tags);
    }

    private void scheduleMemoryTask(long expireTime, String topic, String body, String tags) {
        long delay = expireTime - System.currentTimeMillis();
        if (delay <= 0) {
            // 立即到期，直接投递
            handler.onExpire(topic, body, tags);
            return;
        }
        timer.newTimeout(timeout -> {
            handler.onExpire(topic, body, tags);
        }, delay, TimeUnit.MILLISECONDS);
    }

    // 从文件名 "2026-05-17-16.log" 解析出该小时的时间戳（毫秒）
    private long parseHourFromFileName(String fileName) {
        // 移除 .log 后缀
        String base = fileName.substring(0, fileName.length() - 4);
        String[] parts = base.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);
        int hour = Integer.parseInt(parts[3]);
        java.time.LocalDateTime ldt = java.time.LocalDateTime.of(year, month, day, hour, 0);
        return ldt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}