package com.minmq.common.protocol;

public enum BodyCodec {
    /**
     * 纯文本
     */
    TEXT(1),

    /**
     * json 字符串
     */
    JSON(2),

    /**
     * protobuf 模式
     */
    PROTOBUF(3),
    /**
     * msgpack
     */
    MSGPACK(4),
    /**
     * Avro
     */
    AVRO(5),
    /**
     * Thrift
     */
    THRIFT(6),
    /**
     * byte
     */
    BYTE(7);

    private final int value;

    BodyCodec(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static BodyCodec fromValue(int value) {
        for (BodyCodec mode : values()) {
            if (mode.value == value) return mode;
        }
        return TEXT;
    }
}
