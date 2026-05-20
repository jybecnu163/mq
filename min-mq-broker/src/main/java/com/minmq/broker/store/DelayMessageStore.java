package com.minmq.broker.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 延时消息磁盘存储，按时分片写入文件。
 * 文件格式：delay/yyyy-MM-dd-HH.log
 * 每条记录：[8字节到期时间戳][4字节主题长度][主题字节][4字节body长度][body字节]
 */
public class DelayMessageStore {
    private final String delayDir;

    public DelayMessageStore(String dataDir) {
        this.delayDir = dataDir + "/delay";
        new File(delayDir).mkdirs();
    }

    /**
     * 将一条延时消息持久化到磁盘
     *
     * @param expireTime 到期时间戳（毫秒）
     * @param topic
     * @param body
     * @return 写入的文件路径（用于调试）
     */
    public String store(long expireTime, String topic, byte[] body, String tags) throws IOException {
        // 根据到期时间所属的小时生成文件名
        String hourFile = getHourFile(expireTime);
        File file = new File(delayDir, hourFile);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
             FileChannel channel = raf.getChannel()) {
            channel.position(channel.size()); // 追加到末尾

            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            byte[] tagsBytes = tags != null ? tags.getBytes(StandardCharsets.UTF_8) : new byte[0];

            int totalLen = 8 + 4 + topicBytes.length + 4 + body.length + 4 + tagsBytes.length;
            ByteBuffer buf = ByteBuffer.allocate(totalLen);
            buf.putLong(expireTime);
            buf.putInt(topicBytes.length);
            buf.put(topicBytes);
            buf.putInt(body.length);
            buf.put(body);
            buf.putInt(tagsBytes.length);      // 新增：tags 长度 + 数据
            buf.put(tagsBytes);
            buf.flip();
            channel.write(buf);
        }
        return hourFile;
    }

    /**
     * 加载指定小时文件中的所有延时消息
     *
     * @param hourFile 文件名，如 "2026-05-17-16.log"
     * @return 消息列表
     */
    public List<DelayMessageEntry> loadHourFile(String hourFile) throws IOException {
        List<DelayMessageEntry> entries = new ArrayList<>();
        File file = new File(delayDir, hourFile);
        if (!file.exists()) return entries;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
            while (channel.read(buf) > 0) {
                buf.flip();
                while (buf.remaining() >= 8) {
                    long expireTime = buf.getLong();
                    if (buf.remaining() < 4) break;
                    int topicLen = buf.getInt();
                    if (buf.remaining() < topicLen) break;
                    byte[] topicBytes = new byte[topicLen];
                    buf.get(topicBytes);
                    String topic = new String(topicBytes, StandardCharsets.UTF_8);
                    if (buf.remaining() < 4) break;
                    int bodyLen = buf.getInt();
                    if (buf.remaining() < bodyLen) break;
                    byte[] bodyBytes = new byte[bodyLen];
                    buf.get(bodyBytes);
                    byte[] body = bodyBytes;

                    // 在读取完 body 之后继续读取 tags
                    int tagsLen = buf.getInt();
                    byte[] tagsBytes = new byte[tagsLen];
                    buf.get(tagsBytes);
                    String tags = tagsLen > 0 ? new String(tagsBytes, StandardCharsets.UTF_8) : null;
                    entries.add(new DelayMessageEntry(expireTime, topic, body, tags));
                }
                buf.compact();
            }
        }
        return entries;
    }

    /**
     * 扫描 delay 目录，返回所有未到期的小时文件名（未来还会用到）
     */
    public List<String> listAllHourFiles() {
        File dir = new File(delayDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
        List<String> names = new ArrayList<>();
        if (files != null) {
            for (File f : files) names.add(f.getName());
        }
        return names;
    }

    private String getHourFile(long timestamp) {
        // 将时间戳格式化为 yyyy-MM-dd-HH
        java.time.Instant instant = java.time.Instant.ofEpochMilli(timestamp);
        java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.systemDefault());
        return String.format("%04d-%02d-%02d-%02d.log", ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(), ldt.getHour());
    }

    // 简单的内部类表示一条延时消息记录
    public static class DelayMessageEntry {
        public final long expireTime;
        public final String topic;
        public final byte[] body;
        public final String tags;      // 新增

        public DelayMessageEntry(long expireTime, String topic, byte[] body, String tags) {
            this.expireTime = expireTime;
            this.topic = topic;
            this.body = body;
            this.tags = tags;
        }
    }
}