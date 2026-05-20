package com.minmq.common.protocol;

/**
 * 消息确认模式
 */
public enum AckMode {
    /**
     * 手动确认：业务代码必须显式调用 consumer.ack()
     */
    MANUAL(1),

    /**
     * 客户端自动确认：pull/pullBatch 返回后自动发送 ACK，对业务透明
     */
    CLIENT_AUTO(2),

    /**
     * 投递即确认：服务端将消息推给客户端后立即提交偏移，容忍消息丢失
     */
    SERVER_IMMEDIATE(3);

    private final int value;

    AckMode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static AckMode fromValue(int value) {
        for (AckMode mode : values()) {
            if (mode.value == value) return mode;
        }
        return MANUAL;
    }
}