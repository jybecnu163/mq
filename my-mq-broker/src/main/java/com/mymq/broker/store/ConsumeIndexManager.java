package com.mymq.broker.store;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 消费者索引与进度管理，类似 QMQ 的 pull log 简化版
 * 每个实例对应一个 topic + consumerGroup
 */
public class ConsumeIndexManager {
    private final FileChannel indexChannel;    // 存储物理偏移量，每 8 字节一条
    private final FileChannel progressChannel; // 存储当前消费进度（下一个要消费的索引位置）
    private long consumerOffset;               // 下一个待消费的索引序号
    private long indexCount;                   // 索引文件已有的记录数

    public ConsumeIndexManager(String dataDir, String topic, String group) throws Exception {
        String prefix = dataDir + "/" + topic + "-" + group;

        // 索引文件
        File idxFile = new File(prefix + ".index");
        RandomAccessFile idxRaf = new RandomAccessFile(idxFile, "rw");
        indexChannel = idxRaf.getChannel();
        indexCount = indexChannel.size() / 8;

        // 进度文件
        File progressFile = new File(prefix + ".progress");
        RandomAccessFile progressRaf = new RandomAccessFile(progressFile, "rw");
        progressChannel = progressRaf.getChannel();
        if (progressChannel.size() >= 8) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            progressChannel.read(buf, 0);
            buf.flip();
            consumerOffset = buf.getLong();
        } else {
            consumerOffset = 0;
        }
    }

    /**
     * 追加一条物理偏移量到索引文件
     */
    public synchronized void appendOffset(long physicalOffset) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(physicalOffset);
        buf.flip();
        indexChannel.write(buf, indexCount * 8);
        indexCount++;
    }

    /**
     * 获取下一个待消费消息的物理偏移量，并临时返回其索引位置（用于 ACK 确认）
     * 如果没有新消息返回 -1
     */
    public synchronized long peekNextOffset() {
        if (consumerOffset < indexCount) {
            // 读出该索引位置的物理偏移量
            try {
                ByteBuffer buf = ByteBuffer.allocate(8);
                indexChannel.read(buf, consumerOffset * 8);
                buf.flip();
                return buf.getLong();
            } catch (Exception e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * 获取当前逻辑偏移量（就是 consumerOffset），用于返回给客户端
     */
    public synchronized long getConsumerOffset() {
        return consumerOffset;
    }

    /**
     * 推进消费进度（ACK 确认）
     *
     * @param offset 客户端 ACK 的偏移量，只有当它等于当前 consumerOffset 时才推进
     */
    public synchronized void commitOffset(long offset) throws Exception {
        if (offset == consumerOffset) {
            consumerOffset++;
            // 持久化新进度
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(consumerOffset);
            buf.flip();
            progressChannel.write(buf, 0);
        }
        // 非顺序 ACK 直接忽略，保证顺序消费
    }

    public void close() throws Exception {
        indexChannel.close();
        progressChannel.close();
    }
}