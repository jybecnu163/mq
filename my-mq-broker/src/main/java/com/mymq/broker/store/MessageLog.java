package com.mymq.broker.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * 消息顺序写入日志，类似 QMQ 的 message_log
 */
public class MessageLog {
    private static final Logger log = LoggerFactory.getLogger(MessageLog.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // 段文件最大大小（默认 1 GB）
    private static final long DEFAULT_MAX_SEGMENT_SIZE = 1024L * 1024L * 1024L; // 1GB

    private final String dataDir;
    private final long maxSegmentSize;

    // 所有段（按 baseOffset 升序），包含只读段和当前可写段
    private final List<Segment> segments = new ArrayList<>();
    // 当前正在写入的段（可能为 null，待第一次 append 时创建）
    private Segment activeSegment;

    /**
     * @param dataDir 数据目录
     */
    public MessageLog(String dataDir) throws Exception {
        this(dataDir, DEFAULT_MAX_SEGMENT_SIZE);
    }

    /**
     * @param dataDir        数据目录
     * @param maxSegmentSize 段文件最大大小（字节）
     */
    public MessageLog(String dataDir, long maxSegmentSize) throws Exception {
        this.dataDir = dataDir;

        this.maxSegmentSize = maxSegmentSize;
        init();
    }

    // -------- 初始化：扫描已有段文件 --------
    private void init() throws Exception {
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (files == null) {
            return;
        }

        List<Segment> found = new ArrayList<>();
        for (File file : files) {
            String name = file.getName();
            long baseOffset;
            if ("message.log".equals(name)) {
                // 旧版单文件，视为 baseOffset = 0
                baseOffset = 0;
            } else {
                // 新版段文件：020d.log
                String offsetStr = name.substring(0, name.length() - 4); // 去掉 .log
                baseOffset = Long.parseLong(offsetStr);
            }
            found.add(new Segment(file, baseOffset));
        }

        // 按 baseOffset 排序
        found.sort(Comparator.comparingLong(Segment::getBaseOffset));

        // 检查段之间是否连续（最后一个段除外）
        for (int i = 0; i < found.size() - 1; i++) {
            Segment curr = found.get(i);
            Segment next = found.get(i + 1);
            if (curr.getEndOffset() != next.getBaseOffset()) {
                throw new IllegalStateException(
                        "Segment gap detected: " + curr.getFile().getName() +
                                " ends at " + curr.getEndOffset() +
                                " but next segment starts at " + next.getBaseOffset());
            }
        }

        this.segments.addAll(found);

        // 最后一个段作为活动段（如果存在）
        if (!segments.isEmpty()) {
            activeSegment = segments.get(segments.size() - 1);
            log.info("Recovered {} segments, active segment baseOffset={}, size={}",
                    segments.size(), activeSegment.getBaseOffset(), activeSegment.size());
        } else {
            activeSegment = null;
        }
    }

    // -------- 公开方法 --------

    /**
     * 追加消息，返回全局物理偏移量（写入前的位置）。
     */
    public synchronized long append(String topic, String body, String tags, long timestamp) throws Exception {
        // 需要时创建或切换段
        if (activeSegment == null || activeSegment.size() >= maxSegmentSize) {
            // 首先检查当前活动段是否达到大小上限，若超过则创建新段。
            rollNewSegment();
        }

        // 构造 MessageEntry 对象，包含主题、体、标签和时间戳。
        MessageEntry entry = new MessageEntry(topic, body, tags, timestamp);
        byte[] data = MAPPER.writeValueAsBytes(entry);
        int length = data.length;

        // 写入格式：[4字节数据长度][实际数据]
        // 序列化为 JSON 字节数组，前面加上 4 字节长度头，然后调用 Segment.append 写入
        ByteBuffer buf = ByteBuffer.allocate(4 + length);
        buf.putInt(length);
        buf.put(data);
        buf.flip();

        // 返回的 offset 是全局物理偏移量，即该消息在日志文件中的起始位置。
        return activeSegment.append(buf);
    }

    // 兼容旧调用（无 tags）
    public synchronized long append(String topic, String body, String tags) throws Exception {
        return append(topic, body, tags, System.currentTimeMillis());
    }

    /**
     * 根据全局物理偏移量读取完整消息条目。
     */
    public MessageEntry readMessage(long offset) throws Exception {
        Segment seg = findSegment(offset);
        if (seg == null) {
            throw new IOException("No segment found for offset " + offset);
        }
        long offsetInSeg = offset - seg.getBaseOffset();
        return seg.readMessage(offsetInSeg);
    }

    /**
     * 关闭所有段文件。
     */
    public synchronized void close() {
        for (Segment seg : segments) {
            try {
                seg.close();
            } catch (Exception e) {
                log.error("Failed to close segment {}", seg.getFile().getName(), e);
            }
        }
    }

    /**
     * 获取指定物理偏移量对应消息的时间戳
     */
    public long getMessageTimestamp(long offset) throws Exception {
        Segment seg = findSegment(offset);
        if (seg == null) return 0;
        MessageEntry entry = seg.readMessage(offset - seg.getBaseOffset());
        return entry.timestamp;
    }

    // -------- 私有辅助方法 --------

    /**
     * 创建新段作为活动段。    // 修改 rollNewSegment，关闭旧段
     */
    private void rollNewSegment() throws IOException {
        if (activeSegment != null) {
            // 关闭旧的活动段，释放文件句柄
            activeSegment.close();
        }
        long newBaseOffset = activeSegment == null ? 0 : activeSegment.getEndOffset();
        String fileName = String.format("%020d.log", newBaseOffset);
        File file = new File(dataDir, fileName);
        Segment newSeg = new Segment(file, newBaseOffset);
        segments.add(newSeg);
        activeSegment = newSeg;
        log.info("Created new segment: {}", fileName);
    }

    /**
     * 新增：根据消费进度和时间清理旧段
     * // 删除旧段时增加时间条件：段的最后一条消息时间戳早于指定时间
     *
     * @param minConsumedOffset
     * @param maxAgeMillis
     * @return
     */
    public synchronized int deleteOldSegments(long minConsumedOffset, long maxAgeMillis) {
        long now = System.currentTimeMillis();
        int deleted = 0;
        Iterator<Segment> it = segments.iterator();
        while (it.hasNext()) {
            Segment seg = it.next();
            if (seg == activeSegment) continue;

            // 条件1：所有消息已被消费
            if (seg.getEndOffset() > minConsumedOffset) break;

            // 条件2：段内最后一条消息的时间戳早于 (当前时间 - maxAgeMillis)
            try {
                long lastTimestamp = seg.getLastTimestamp();
                if (lastTimestamp > 0 && (now - lastTimestamp) < maxAgeMillis) {
                    break; // 后续段可能更晚，不能删除
                }
            } catch (Exception e) {
                log.warn("Failed to read last timestamp of segment {}", seg.getFile().getName(), e);
            }

            // 安全删除
            if (seg.channel != null && seg.channel.isOpen()) {
                try {
                    seg.close();
                } catch (Exception e) {
                    log.error("close error", e);
                }
            }
            if (seg.file.exists()) {
                boolean ok = seg.file.delete();
                if (ok) log.info("Deleted old segment: {}", seg.file.getName());
                else log.warn("Failed to delete: {}", seg.file.getName());
            }
            it.remove();
            deleted++;
        }
        return deleted;
    }

    /**
     * 根据全局偏移量查找对应的段。
     */
    private Segment findSegment(long offset) {
        // 二分查找
        int low = 0, high = segments.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Segment seg = segments.get(mid);
            if (offset < seg.getBaseOffset()) {
                high = mid - 1;
            } else if (offset >= seg.getEndOffset()) {
                low = mid + 1;
            } else {
                return seg;
            }
        }
        return null;
    }

    // -------- 内部类：段 --------
    private static class Segment {
        private final File file;
        private final long baseOffset;       // 段起始全局偏移量
        private FileChannel channel;   // 读写通道（读写模式）
        private long wrotePosition;          // 当前已写入字节数（仅活动段有意义）
        private volatile long lastModified;  // 段关闭时的系统时间，未关闭时可能为0
        private long lastTimestamp;  // 段内最后一条消息的时间戳（缓存）

        Segment(File file, long baseOffset) throws IOException {
            this.file = file;
            this.baseOffset = baseOffset;
            // 以读写模式打开，支持追加和读取
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();
            // 启动时，定位到文件末尾继续写入
            this.wrotePosition = channel.size();
            // 将写入位置移到文件末尾
            channel.position(wrotePosition);
            // 初始为文件系统时间
            this.lastModified = file.lastModified();

            // 尝试读取段内最后一条消息的时间戳，缓存起来
            try {
                if (wrotePosition > 0) {
                    MessageEntry lastEntry = readMessage(wrotePosition -
                            (4 + getLastEntrySize()));
                    this.lastTimestamp = lastEntry.timestamp;
                }
            } catch (Exception e) {
                this.lastTimestamp = 0; // 兼容旧格式
            }
        }

        long getBaseOffset() {
            return baseOffset;
        }

        long getEndOffset() {
            return baseOffset + wrotePosition;
        }

        File getFile() {
            return file;
        }

        long size() {
            return wrotePosition;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        public long getWrotePosition() {
            return wrotePosition;
        }

        public void setWrotePosition(long wrotePosition) {
            this.wrotePosition = wrotePosition;
        }

        public FileChannel getChannel() {
            return channel;
        }

        public void setChannel(FileChannel channel) {
            this.channel = channel;
        }

        public long getLastTimestamp() {
            return lastTimestamp;
        }

        public void setLastTimestamp(long lastTimestamp) {
            this.lastTimestamp = lastTimestamp;
        }

        /**
         * 追加数据并返回全局偏移量。
         */
        synchronized long append(ByteBuffer buf) throws IOException {
            // 返回给调用者的全局偏移量
            long offset = getEndOffset();
            channel.write(buf);
            // buf 已经被写入，limit 等于长度
            wrotePosition += buf.limit();

            // 更新最后修改时间（也可依赖操作系统，这里显式设置）
            this.lastModified = System.currentTimeMillis();
            try {
                // 更新缓存的时间戳（需要解析本次写入的消息）
                // 简单方式：每次追加后，解析最后一条消息获取时间戳
                MessageEntry entry = readMessage(wrotePosition - (4 + buf.limit() - 4));
                this.lastTimestamp = entry.timestamp;
            } catch (Exception e) { /* ignore */ }
            return offset;
        }

        // 帮助方法：获取最后一条消息的数据长度（用于定位）
        private int getLastEntrySize() throws IOException {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, wrotePosition - 4);
            lenBuf.flip();
            return lenBuf.getInt();
        }

        /**
         * 读取段内偏移量处的消息条目。
         */
        MessageEntry readMessage(long offsetInSeg) throws IOException {
            // 读取长度
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, offsetInSeg);
            lenBuf.flip();
            int length = lenBuf.getInt();

            // 读取数据
            ByteBuffer dataBuf = ByteBuffer.allocate(length);
            channel.read(dataBuf, offsetInSeg + 4);
            dataBuf.flip();
            byte[] data = new byte[length];
            dataBuf.get(data);

            return MAPPER.readValue(data, MessageEntry.class);
        }

        synchronized void close() throws IOException {
            if (channel != null && channel.isOpen()) {
                channel.close();
                channel = null;
                // 记录关闭时刻
                this.lastModified = System.currentTimeMillis();
            }
        }


    }

    // -------- 内部消息条目（保持不变） --------
    // 内部序列化结构，仅保存 topic 与 body（后续可扩展属性）
    public static class MessageEntry {
        private String topic;
        private String body;
        private String tags;  // 新增
        private long timestamp;

        public MessageEntry() {
            // 无参构造器
        }

        public MessageEntry(long timestamp) {
            this.timestamp = timestamp;
        }

        public MessageEntry(String topic, String body, String tags, long timestamp) {
            this.topic = topic;
            this.body = body;
            this.tags = tags;
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }


        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
        }
    }
}