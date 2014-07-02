package com.facebook.presto.kafka;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.nio.ByteBuffer;

public class NumberEncoder
        implements Encoder<Number>
{
    public NumberEncoder(VerifiableProperties props)
    {
    }

    @Override
    public byte[] toBytes(Number value)
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value == null ? 0L : value.longValue());
        return buf.array();
    }
}
