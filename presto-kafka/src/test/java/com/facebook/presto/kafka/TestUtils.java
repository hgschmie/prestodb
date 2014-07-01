package com.facebook.presto.kafka;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public final class TestUtils
{
    private TestUtils()
    {
        throw new AssertionError("Do not instantiate");
    }

    public static int findUnusedPort()
            throws IOException
    {
        int port;

        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(0));
            port = socket.getLocalPort();
        }

        return port;
    }
}
