package com.facebook.presto.kafka;

import com.facebook.presto.spi.ErrorCode;

/**
 * Kafka connector specific error codes.
 */
public enum KafkaErrorCode
{
    // Connectors can use error codes starting at EXTERNAL

    /**
     * A requested data conversion is not supported.
     */
    KAFKA_CONVERSION_NOT_SUPPORTED(0x0100_0000);

    private final ErrorCode errorCode;

    KafkaErrorCode(int code)
    {
        errorCode = new ErrorCode(code, name());
    }

    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
