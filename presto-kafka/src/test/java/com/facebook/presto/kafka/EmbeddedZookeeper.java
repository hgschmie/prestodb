package com.facebook.presto.kafka;

import com.google.common.io.Files;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

public class EmbeddedZookeeper
        implements Closeable
{
    private final int port;
    private final File zkDataDir;
    private final ZooKeeperServer zkServer;
    private final NIOServerCnxn.Factory cnxnFactory;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public EmbeddedZookeeper(int port)
            throws IOException
    {
        this.port = port;
        zkDataDir = Files.createTempDir();
        zkServer = new ZooKeeperServer();

        FileTxnSnapLog ftxn = new FileTxnSnapLog(zkDataDir, zkDataDir);
        zkServer.setTxnLogFactory(ftxn);

        cnxnFactory = new NIOServerCnxn.Factory(new InetSocketAddress(this.port), 0);
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            cnxnFactory.startup(zkServer);
        }
    }

    @Override
    public void close()
    {
        if (started.get() && !stopped.getAndSet(true)) {
            cnxnFactory.shutdown();
            try {
                cnxnFactory.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (zkServer.isRunning()) {
                zkServer.shutdown();
            }
        }
    }

    public String getConnectString()
    {
        return "localhost:" + Integer.toString(port);
    }

    public void cleanup()
    {
        checkState(stopped.get(), "not stopped");

        for (File file : Files.fileTreeTraverser().postOrderTraversal(zkDataDir)) {
            file.delete();
        }
    }
}
