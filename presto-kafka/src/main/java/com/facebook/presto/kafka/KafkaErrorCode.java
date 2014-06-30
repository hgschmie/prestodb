package com.facebook.presto.kafka;

import com.facebook.presto.spi.ErrorCode;

public enum KafkaErrorCode
{
    // Connectors can use error codes starting at EXTERNAL

    /**
     * A requested operation is not supported by the connector.
     */
    KAFKA_OPERATION_NOT_SUPPORTED(0x0100_0000),

    /**
     * A requested data conversion is not supported.
     */
    KAFKA_CONVERSION_NOT_SUPPORTED(0x0100_0001);

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
