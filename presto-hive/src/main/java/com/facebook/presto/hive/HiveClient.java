package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncRecursiveWalker;
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.FileStatusCallback;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.spi.SchemaField.Type;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat.SymlinkTextInputSplit;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HivePartitionChunk.makeLastChunk;

import static com.facebook.presto.hive.HiveUtil.convertHiveType;
import static com.facebook.presto.hive.HiveUtil.convertNativeHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;


@SuppressWarnings("deprecation")
public class HiveClient
        implements ImportClient
{
    public static final String HIVE_VIEWS_NOT_SUPPORTED = "Hive views are not supported";

    private static final Logger log = Logger.get(HiveClient.class);

    private final long maxChunkBytes;
    private final int maxOutstandingChunks;
    private final int maxChunkIteratorThreads;
    private final int partitionBatchSize;
    private final HiveChunkEncoder hiveChunkEncoder;
    private final CachingHiveMetastore metastore;
    private final FileSystemCache fileSystemCache;
    private final ExecutorService executor;

    public HiveClient(
            long maxChunkBytes,
            int maxOutstandingChunks,
            int maxChunkIteratorThreads,
            int partitionBatchSize,
            HiveChunkEncoder hiveChunkEncoder,
            CachingHiveMetastore metastore,
            FileSystemCache fileSystemCache,
            ExecutorService executor)
    {
        this.maxChunkBytes = maxChunkBytes;
        this.maxOutstandingChunks = maxOutstandingChunks;
        this.maxChunkIteratorThreads = maxChunkIteratorThreads;
        this.partitionBatchSize = partitionBatchSize;
        this.hiveChunkEncoder = hiveChunkEncoder;
        this.metastore = metastore;
        this.fileSystemCache = fileSystemCache;
        this.executor = executor;
    }

    @Override
    public List<String> getDatabaseNames()
    {
        return metastore.getAllDatabases();
    }

    @Override
    public List<String> getTableNames(String databaseName)
            throws ObjectNotFoundException
    {
        try {
            return metastore.getAllTables(databaseName);
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
    }

    @Override
    public List<SchemaField> getTableSchema(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try {
            Table table = metastore.getTable(databaseName, tableName);
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            Properties schema = MetaStoreUtils.getSchema(table);
            return getSchemaFields(schema, partitionKeys);
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (MetaException | SerDeException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaField> getPartitionKeys(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try {
            Table table = metastore.getTable(databaseName, tableName);
            List<FieldSchema> partitionKeys = table.getPartitionKeys();

            ImmutableList.Builder<SchemaField> schemaFields = ImmutableList.builder();
            for (int i = 0; i < partitionKeys.size(); i++) {
                FieldSchema field = partitionKeys.get(i);
                Type type = convertHiveType(field.getType());

                // partition keys are always the first fields in the table
                schemaFields.add(SchemaField.createPrimitive(field.getName(), i, type));
            }

            return schemaFields.build();
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try {
            return metastore.getPartitionNames(databaseName, tableName);
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName, final Map<String, Object> filters)
            throws ObjectNotFoundException
    {
        // build the filtering prefix
        List<String> parts = new ArrayList<>();
        List<SchemaField> partitionKeys = getPartitionKeys(databaseName, tableName);
        for (SchemaField key : partitionKeys) {
            Object value = filters.get(key.getFieldName());

            if (value == null) {
                // we're building a partition prefix, so stop at the first missing binding
                break;
            }

            Preconditions.checkArgument(value instanceof String || value instanceof Double || value instanceof Long,
                    "Only String, Double and Long partition keys are supported");

            parts.add(value.toString());
        }

        // fetch the partition names
        List<PartitionInfo> partitions;
        if (parts.isEmpty()) {
            partitions = getPartitions(databaseName, tableName);
        }
        else {
            try {
                List<String> names = metastore.getPartitionNamesByParts(databaseName, tableName, parts);
                partitions = Lists.transform(names, toPartitionInfo(partitionKeys));
            }
            catch (NoSuchObjectException e) {
                throw new ObjectNotFoundException(e.getMessage());
            }
        }

        // do a final pass to filter based on fields that could not be used to build the prefix
        return ImmutableList.copyOf(Iterables.filter(partitions, partitionMatches(filters)));
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        List<SchemaField> partitionKeys = getPartitionKeys(databaseName, tableName);
        return Lists.transform(getPartitionNames(databaseName, tableName), toPartitionInfo(partitionKeys));
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
            throws ObjectNotFoundException
    {
        return getPartitionChunks(databaseName, tableName, ImmutableList.of(partitionName), columns);
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
            throws ObjectNotFoundException
    {
        Table table;
        Iterable<Partition> partitions;
        try {
            table = metastore.getTable(databaseName, tableName);
            partitions = getPartitions(databaseName, tableName, partitionNames);
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }

        return getPartitionChunks(table, ImmutableList.copyOf(partitionNames), partitions, getHiveColumns(table, columns));
    }

    private Iterable<Partition> getPartitions(final String databaseName, final String tableName, final List<String> partitionNames)
            throws NoSuchObjectException
    {
        if (partitionNames.equals(ImmutableList.of(UnpartitionedPartition.UNPARTITIONED_NAME))) {
            return ImmutableList.<Partition>of(UnpartitionedPartition.INSTANCE);
        }

        Iterable<List<String>> partitionNameBatches = partition(partitionNames, partitionBatchSize);
        Iterable<List<Partition>> partitionBatches = transform(partitionNameBatches, new Function<List<String>, List<Partition>>()
        {
            @Override
            public List<Partition> apply(List<String> partitionNameBatch)
            {
                Exception exception = null;
                for (int attempt = 0; attempt < 10; attempt++) {
                    try {
                        List<Partition> partitions = metastore.getPartitionsByNames(databaseName, tableName, partitionNameBatch);
                        checkState(partitionNameBatch.size() == partitions.size(), "expected %s partitions but found %s", partitionNameBatch.size(), partitions.size());
                        return partitions;
                    }
                    catch (NoSuchObjectException | NullPointerException | IllegalStateException | IllegalArgumentException e) {
                        throw Throwables.propagate(e);
                    }
                    catch (Exception e) {
                        exception = e;
                        log.debug("getPartitions attempt %s failed, will retry. Exception: %s", attempt, e.getMessage());
                    }

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
                throw Throwables.propagate(exception);
            }
        });
        return concat(partitionBatches);
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        assert partitionChunk instanceof HivePartitionChunk; // IDEA-60343
        return HiveChunkReader.getRecords(fileSystemCache.getConfiguration(), (HivePartitionChunk) partitionChunk);
    }

    @Override
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        assert partitionChunk instanceof HivePartitionChunk; // IDEA-60343
        return hiveChunkEncoder.serialize((HivePartitionChunk) partitionChunk);
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        return hiveChunkEncoder.deserialize(bytes);
    }

    private static List<HiveColumn> getHiveColumns(Table table, List<String> columns)
    {
        HashSet<String> columnNames = new HashSet<>(columns);

        // remove primary keys
        for (FieldSchema fieldSchema : table.getPartitionKeys()) {
            columnNames.remove(fieldSchema.getName());
        }

        try {
            Properties schema = MetaStoreUtils.getSchema(table);
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
            StructObjectInspector tableInspector = (StructObjectInspector) deserializer.getObjectInspector();

            ImmutableList.Builder<HiveColumn> hiveColumns = ImmutableList.builder();
            int index = 0;
            for (StructField field : tableInspector.getAllStructFieldRefs()) {
                // ignore unused columns
                // remove the columns as we find them so we can know if all columns were found
                if (columnNames.remove(field.getFieldName())) {
                    ObjectInspector fieldInspector = field.getFieldObjectInspector();
                    HiveType hiveType = HiveType.getSupportedHiveType(fieldInspector);
                    hiveColumns.add(new HiveColumn(field.getFieldName(), index, hiveType));
                }
                index++;
            }

            Preconditions.checkArgument(columnNames.isEmpty(), "Table %s does not contain the columns %s", table.getTableName(), columnNames);

            return hiveColumns.build();
        }
        catch (MetaException | SerDeException e) {
            throw Throwables.propagate(e);
        }
    }

    private Iterable<PartitionChunk> getPartitionChunks(Table table, Iterable<String> partitionNames, Iterable<Partition> partitions, List<HiveColumn> columns)
    {
        return new PartitionChunkIterable(table, partitionNames, partitions, columns, maxChunkBytes, maxOutstandingChunks, maxChunkIteratorThreads, fileSystemCache, executor, partitionBatchSize);
    }

    private static class PartitionChunkIterable
            implements Iterable<PartitionChunk>
    {
        private static final PartitionChunk FINISHED_MARKER = new PartitionChunk()
        {
            @Override
            public String getPartitionName()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLength()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<InetAddress> getHosts()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getInfo()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isLastChunk()
            {
                throw new UnsupportedOperationException();
            }
        };

        private final Table table;
        private final Iterable<String> partitionNames;
        private final Iterable<Partition> partitions;
        private final List<HiveColumn> columns;
        private final long maxChunkBytes;
        private final int maxOutstandingChunks;
        private final int maxThreads;
        private final FileSystemCache fileSystemCache;
        private final ExecutorService executor;
        private final ClassLoader classLoader;
        private final int partitionBatchSize;

        private PartitionChunkIterable(Table table,
                Iterable<String> partitionNames,
                Iterable<Partition> partitions,
                List<HiveColumn> columns,
                long maxChunkBytes,
                int maxOutstandingChunks,
                int maxThreads,
                FileSystemCache fileSystemCache,
                ExecutorService executor,
                int partitionBatchSize)
        {
            this.table = table;
            this.partitionNames = partitionNames;
            this.partitions = partitions;
            this.partitionBatchSize = partitionBatchSize;
            this.columns = ImmutableList.copyOf(columns);
            this.maxChunkBytes = maxChunkBytes;
            this.maxOutstandingChunks = maxOutstandingChunks;
            this.maxThreads = maxThreads;
            this.fileSystemCache = fileSystemCache;
            this.executor = executor;
            this.classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public Iterator<PartitionChunk> iterator()
        {
            // Each iterator has its own bounded executor and can be independently suspended
            final SuspendingExecutor suspendingExecutor = new SuspendingExecutor(new BoundedExecutor(executor, maxThreads));
            final PartitionChunkQueue partitionChunkQueue = new PartitionChunkQueue(maxOutstandingChunks, suspendingExecutor);
            executor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    loadPartitionChunks(partitionChunkQueue, suspendingExecutor);
                    return null;
                }
            });
            return partitionChunkQueue;
        }

        private void loadPartitionChunks(final PartitionChunkQueue partitionChunkQueue, final SuspendingExecutor suspendingExecutor)
                throws InterruptedException
        {
            final Semaphore semaphore = new Semaphore(partitionBatchSize);
            try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
                ImmutableList.Builder<ListenableFuture<Void>> futureBuilder = ImmutableList.builder();

                Iterator<String> nameIterator = partitionNames.iterator();
                for (Partition partition : partitions) {
                    checkState(nameIterator.hasNext(), "different number of partitions and partition names!");
                    semaphore.acquire();
                    final String partitionName = nameIterator.next();
                    final Properties schema = getPartitionSchema(table, partition);
                    final List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition);
                    final InputFormat<?, ?> inputFormat = getInputFormat(fileSystemCache.getConfiguration(), schema, false);
                    Path partitionPath = new CachingPath(getPartitionLocation(table, partition), fileSystemCache);

                    final FileSystem fs = partitionPath.getFileSystem(fileSystemCache.getConfiguration());
                    final PartitionChunkPoisoner chunkPoisoner = new PartitionChunkPoisoner(partitionChunkQueue);

                    if (inputFormat instanceof SymlinkTextInputFormat) {
                        JobConf jobConf = new JobConf(fileSystemCache.getConfiguration());
                        FileInputFormat.setInputPaths(jobConf, partitionPath);
                        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
                        for (InputSplit rawSplit : splits) {
                            FileSplit split = ((SymlinkTextInputSplit) rawSplit).getTargetSplit();
                            chunkPoisoner.writeChunks(createHivePartitionChunks(partitionName,
                                    fs.getFileStatus(split.getPath()),
                                    split.getStart(),
                                    split.getLength(),
                                    schema,
                                    partitionKeys,
                                    fs,
                                    false));
                        }
                        chunkPoisoner.finish();
                        continue;
                    }

                    ListenableFuture<Void> partitionFuture = new AsyncRecursiveWalker(fs, suspendingExecutor).beginWalk(partitionPath, new FileStatusCallback()
                    {
                        @Override
                        public void process(FileStatus file)
                        {
                            try {
                                boolean splittable = isSplittable(inputFormat,
                                        file.getPath().getFileSystem(fileSystemCache.getConfiguration()),
                                        file.getPath());

                                chunkPoisoner.writeChunks(createHivePartitionChunks(partitionName, file, 0, file.getLen(), schema, partitionKeys, fs, splittable));
                            }
                            catch (IOException e) {
                                partitionChunkQueue.fail(e);
                            }
                        }
                    });

                    // release the semaphore when the partition finishes
                    Futures.addCallback(partitionFuture, new FutureCallback<Void>()
                    {
                        @Override
                        public void onSuccess(Void result)
                        {
                            chunkPoisoner.finish();
                            semaphore.release();
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            chunkPoisoner.finish();
                            semaphore.release();
                        }
                    });
                    futureBuilder.add(partitionFuture);
                }

                // when all partitions finish, mark the queue as finished
                Futures.addCallback(Futures.allAsList(futureBuilder.build()), new FutureCallback<List<Void>>()
                {
                    @Override
                    public void onSuccess(List<Void> result)
                    {
                        partitionChunkQueue.finished();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        partitionChunkQueue.fail(t);
                    }
                });
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        private List<HivePartitionChunk> createHivePartitionChunks(
                String partitionName,
                FileStatus file,
                long start,
                long length,
                Properties schema,
                List<HivePartitionKey> partitionKeys,
                FileSystem fs,
                boolean splittable)
                throws IOException
        {
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(file, start, length);

            ImmutableList.Builder<HivePartitionChunk> builder = ImmutableList.builder();
            if (splittable) {
                for (BlockLocation blockLocation : fileBlockLocations) {
                    builder.add(new HivePartitionChunk(partitionName,
                            false,
                            file.getPath(),
                            blockLocation.getOffset(),
                            blockLocation.getLength(),
                            schema,
                            partitionKeys,
                            columns,
                            toInetAddress(blockLocation.getHosts())));
                }
            } else {
                // not splittable, use the hosts from the first block
                builder.add(new HivePartitionChunk(
                        partitionName,
                        false,
                        file.getPath(),
                        start,
                        length,
                        schema,
                        partitionKeys,
                        columns,
                        toInetAddress(fileBlockLocations[0].getHosts())));
            }
            return builder.build();
        }

        private List<InetAddress> toInetAddress(String[] hosts)
        {
            ImmutableList.Builder<InetAddress> builder = ImmutableList.builder();
            for (String host : hosts) {
                try {
                    builder.add(InetAddress.getByName(host));
                }
                catch (UnknownHostException e) {
                }
            }
            return builder.build();
        }

        /**
         * Holds onto the last seen partition chunk for a given partition and
         * will "poison" it with an end marker when a partition is finished.
         */
        private static class PartitionChunkPoisoner
        {
            private final PartitionChunkQueue partitionChunkQueue;

            private AtomicReference<HivePartitionChunk> chunkHolder = new AtomicReference<>();
            private AtomicBoolean done = new AtomicBoolean();

            private PartitionChunkPoisoner(PartitionChunkQueue partitionChunkQueue)
            {
                this.partitionChunkQueue = checkNotNull(partitionChunkQueue, "partitionChunkQueue is null");
            }

            public synchronized void writeChunks(Iterable<HivePartitionChunk> chunks)
            {
                checkNotNull(chunks, "chunks is null");
                checkState(!done.get(), "already done");

                for(Iterator<HivePartitionChunk> it = chunks.iterator(); it.hasNext(); ) {
                    HivePartitionChunk previousChunk = chunkHolder.getAndSet(it.next());
                    if (previousChunk != null) {
                        partitionChunkQueue.addToQueue(previousChunk);
                    }
                }
            }

            private synchronized void finish()
            {
                checkState(!done.getAndSet(true), "already done");
                HivePartitionChunk finalChunk = chunkHolder.getAndSet(null);
                if (finalChunk != null) {
                    partitionChunkQueue.addToQueue(makeLastChunk(finalChunk));
                }
            }
        }

        private static class PartitionChunkQueue
                extends AbstractIterator<PartitionChunk>
        {
            private final BlockingQueue<PartitionChunk> queue = new LinkedBlockingQueue<>();
            private final AtomicInteger outstandingChunkCount = new AtomicInteger();
            private final AtomicReference<Throwable> throwable = new AtomicReference<>();
            private final int maxOutstandingChunks;
            private final SuspendingExecutor suspendingExecutor;

            private PartitionChunkQueue(int maxOutstandingChunks, SuspendingExecutor suspendingExecutor)
            {
                this.maxOutstandingChunks = maxOutstandingChunks;
                this.suspendingExecutor = suspendingExecutor;
            }

            private void addToQueue(PartitionChunk chunk)
            {
                queue.add(chunk);
                if (outstandingChunkCount.incrementAndGet() == maxOutstandingChunks) {
                    suspendingExecutor.suspend();
                }
            }

            private void finished()
            {
                queue.add(FINISHED_MARKER);
            }

            private void fail(Throwable e)
            {
                throwable.set(e);
            }

            @Override
            protected PartitionChunk computeNext()
            {
                if (throwable.get() != null) {
                    throw Throwables.propagate(throwable.get());
                }

                try {
                    PartitionChunk chunk = queue.take();
                    if (chunk == FINISHED_MARKER) {
                        return endOfData();
                    }
                    if (outstandingChunkCount.getAndDecrement() == maxOutstandingChunks) {
                        suspendingExecutor.resume();
                    }
                    return chunk;
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private static boolean isSplittable(InputFormat<?,?> inputFormat, FileSystem fileSystem, Path path)
    {
        // use reflection to get isSplitable method on InputFormat
        Method method = null;
        for (Class<?> clazz = inputFormat.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            try {
                method = clazz.getDeclaredMethod("isSplitable", FileSystem.class, Path.class);
                break;
            }
            catch (NoSuchMethodException e) {
            }
        }

        if (method == null) {
            return false;
        }
        try {
            method.setAccessible(true);
            return (boolean) method.invoke(inputFormat, fileSystem, path);
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Function<String, PartitionInfo> toPartitionInfo(final List<SchemaField> keys)
    {
        return new Function<String, PartitionInfo>()
        {
            @Override
            public PartitionInfo apply(String partitionName)
            {
                if (partitionName.equals(UnpartitionedPartition.UNPARTITIONED_NAME)) {
                    return new PartitionInfo(UnpartitionedPartition.UNPARTITIONED_NAME);
                }

                try {
                    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                    List<String> parts = Warehouse.getPartValuesFromPartName(partitionName);
                    for (int i = 0; i < parts.size(); i++) {
                        builder.put(keys.get(i).getFieldName(), parts.get(i));
                    }

                    return new PartitionInfo(partitionName, builder.build());
                }
                catch (MetaException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static Predicate<PartitionInfo> partitionMatches(final Map<String, Object> filters)
    {
        return new Predicate<PartitionInfo>()
        {
            @Override
            public boolean apply(PartitionInfo partition)
            {
                for (Map.Entry<String, String> entry : partition.getKeyFields().entrySet()) {
                    String partitionKey = entry.getKey();
                    Object filterValue = filters.get(partitionKey);
                    if (filterValue != null && !entry.getValue().equals(filterValue)) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    private static List<SchemaField> getSchemaFields(Properties schema, List<FieldSchema> partitionKeys)
            throws MetaException, SerDeException
    {
        Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
        ObjectInspector inspector = deserializer.getObjectInspector();
        checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
        StructObjectInspector structObjectInspector = (StructObjectInspector) inspector;

        ImmutableList.Builder<SchemaField> schemaFields = ImmutableList.builder();

        // add the partition keys
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);
            SchemaField.Type type = convertHiveType(field.getType());
            schemaFields.add(SchemaField.createPrimitive(field.getName(), i, type));
        }

        // add the data fields
        List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
        int columnIndex = partitionKeys.size();
        for (StructField field : fields) {
            ObjectInspector fieldInspector = field.getFieldObjectInspector();

            // ignore unsupported types rather than failing
            HiveType hiveType = HiveType.getHiveType(fieldInspector);
            if (hiveType != null) {
                schemaFields.add(SchemaField.createPrimitive(field.getFieldName(), columnIndex, hiveType.getNativeType()));
            }

            columnIndex++;
        }

        return schemaFields.build();
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Partition partition)
    {
        if (partition instanceof UnpartitionedPartition) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<FieldSchema> keys = table.getPartitionKeys();
        List<String> values = partition.getValues();
        checkArgument(keys.size() == values.size(), "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            PrimitiveCategory primitiveCategory = convertNativeHiveType(keys.get(i).getType());
            HiveType hiveType = HiveType.getSupportedHiveType(primitiveCategory);
            String value = values.get(i);
            checkNotNull(value, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, hiveType, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Partition partition)
    {
        if (partition instanceof UnpartitionedPartition) {
            return MetaStoreUtils.getSchema(table);
        }
        return MetaStoreUtils.getSchema(partition, table);
    }

    private static String getPartitionLocation(Table table, Partition partition)
    {
        if (partition instanceof UnpartitionedPartition) {
            return table.getSd().getLocation();
        }
        return partition.getSd().getLocation();
    }

    private static class ThreadContextClassLoader
            implements Closeable
    {
        private final ClassLoader originalThreadContextClassLoader;

        private ThreadContextClassLoader(ClassLoader newThreadContextClassLoader)
        {
            this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
        }

        @Override
        public void close()
        {
            Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
        }
    }
}
