package com.mymq.broker.store;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 消息顺序写入日志，类似 QMQ 的 message_log
 */
public class MessageLog {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final FileChannel channel;
    private long wrotePosition;  // 当前写入位置，即下一条消息的起始物理偏移量

    public MessageLog(String dataDir) throws Exception {
        File dir = new File(dataDir);
        if (!dir.exists()) dir.mkdirs();
        File file = new File(dataDir, "message.log");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        // 从文件末尾继续写入（支持重启恢复）
        wrotePosition = channel.size();
        channel.position(wrotePosition);
    }

    /**
     * 追加消息，返回全局物理偏移量（写入前的位置）
     */
    public synchronized long append(String topic, String body) throws Exception {
        MessageEntry entry = new MessageEntry(topic, body);
        byte[] data = MAPPER.writeValueAsBytes(entry);
        int length = data.length;

        // 写入格式：[4字节数据长度][实际数据]
        ByteBuffer buf = ByteBuffer.allocate(4 + length);
        buf.putInt(length);
        buf.put(data);
        buf.flip();

        long offset = wrotePosition;
        channel.write(buf);
        wrotePosition += (4 + length);
        return offset;
    }

    /**
     * 根据物理偏移量读取消息 body（仅返回 body 内容）
     */
    public String readMessage(long offset) throws Exception {
        // 读取长度
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        channel.read(lenBuf, offset);
        lenBuf.flip();
        int length = lenBuf.getInt();

        // 读取数据
        ByteBuffer dataBuf = ByteBuffer.allocate(length);
        channel.read(dataBuf, offset + 4);
        dataBuf.flip();
        byte[] data = new byte[length];
        dataBuf.get(data);

        MessageEntry entry = MAPPER.readValue(data, MessageEntry.class);
        return entry.body;
    }

    public void close() throws Exception {
        channel.close();
    }

    // 内部序列化结构，仅保存 topic 与 body（后续可扩展属性）
    private static class MessageEntry {
        public String topic;
        public String body;

        public MessageEntry() {
        }

        public MessageEntry(String topic, String body) {
            this.topic = topic;
            this.body = body;
        }
    }
}