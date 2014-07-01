package com.facebook.presto.kafka;

import com.google.common.primitives.Ints;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class NumberPartitioner
        implements Partitioner
{
    public NumberPartitioner(VerifiableProperties props)
    {
    }

    @Override
    public int partition(Object key, int numPartitions)
    {
        if (key instanceof Number) {
            return Ints.checkedCast(((Number) key).longValue() % numPartitions);
        }
        else {
            return 0;
        }
    }
}
