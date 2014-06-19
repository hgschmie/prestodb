package com.facebook.presto.kafka;

import com.google.common.primitives.Ints;

import kafka.javaapi.consumer.SimpleConsumer;

public class CloseableSimpleConsumer extends SimpleConsumer implements AutoCloseable
{
    CloseableSimpleConsumer(String host,
                            int port,
                            long soTimeout,
                            long bufferSize,
                            String clientId)
    {
        super(host, port, Ints.checkedCast(soTimeout), Ints.checkedCast(bufferSize), clientId);
    }

    @Override
    public void close()
    {
        super.close();
    }
}
