package com.facebook.presto.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.io.IOException;

public class JsonEncoder
        implements Encoder<Object>
{
    private final ObjectMapper objectMapper;

    public JsonEncoder(VerifiableProperties props)
    {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] toBytes(Object o)
    {
        try {
            return objectMapper.writeValueAsBytes(o);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}

