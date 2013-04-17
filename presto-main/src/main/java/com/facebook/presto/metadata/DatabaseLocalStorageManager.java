package com.facebook.presto.metadata;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.serde.BlocksFileStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;

public class DatabaseLocalStorageManager
        implements LocalStorageManager
{
    private static final boolean ENABLE_OPTIMIZATION = Boolean.valueOf("false");

    private static final BlocksFileEncoding DEFAULT_ENCODING = BlocksFileEncoding.SNAPPY;

    private static final int RUN_LENGTH_AVERAGE_CUTOFF = 3;
    private static final int DICTIONARY_CARDINALITY_CUTOFF = 1000;

    private final IDBI dbi;
    private final File baseStorageDir;
    private final File baseStagingDir;
    private final StorageManagerDao dao;

    private final LoadingCache<File, Slice> mappedFileCache = CacheBuilder.newBuilder().build(new CacheLoader<File, Slice>()
    {
        @Override
        public Slice load(File file)
                throws Exception
        {
            checkArgument(file.isAbsolute(), "file is not absolute");
            if (file.exists() && file.length() > 0) {
                return Slices.mapFileReadOnly(file);
            }
            else {
                return Slices.EMPTY_SLICE;
            }
        }
    });

    @Inject
    public DatabaseLocalStorageManager(@ForStorageManager IDBI dbi, DatabaseLocalStorageManagerConfig config)
            throws IOException
    {
        checkNotNull(config, "config is null");
        File baseDataDir = checkNotNull(config.getDataDirectory(), "dataDirectory is null");
        this.baseStorageDir = createDirectory(new File(baseDataDir, "storage"));
        this.baseStagingDir = createDirectory(new File(baseDataDir, "staging"));
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(StorageManagerDao.class);

        dao.createTableColumns();
    }

    @Override
    public void importShard(long shardId, List<? extends ColumnHandle> columnHandles, Operator source)
            throws IOException
    {
        checkArgument(source.getChannelCount() == columnHandles.size(), "channel count does not match columnHandles list");
        checkState(!shardExists(shardId), "shard %s has already been imported", shardId);

        ColumnFileHandle fileHandle = createStagingFileHandles(shardId, columnHandles);

        // Locally stage the imported data
        importData(source, fileHandle);

        commit(fileHandle);
    }

    @Override
    public ColumnFileHandle createStagingFileHandles(long shardId, List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        File shardPath = getShardPath(baseStagingDir, shardId);

        ColumnFileHandle.Builder builder = ColumnFileHandle.builder(shardId);

        for (ColumnHandle columnHandle : columnHandles) {
            File file = getColumnFile(shardPath, columnHandle, DEFAULT_ENCODING);
            Files.createParentDirs(file);
            builder.addColumn(columnHandle, file, DEFAULT_ENCODING);
        }

        return builder.build();
    }

    @Override
    public void commit(ColumnFileHandle columnFileHandle)
            throws IOException
    {
        checkNotNull(columnFileHandle, "columnFileHandle is null");

        columnFileHandle.commit();

        // Process staged files to optimize encodings if necessary
        ColumnFileHandle finalColumnFileHandle = optimizeEncodings(columnFileHandle);

        // Commit all the columns at the same time once everything has been successfully imported
        commitShardColumns(finalColumnFileHandle);

        // Delete empty staging directory
        deleteStagingDirectory(columnFileHandle);
    }

    private ColumnFileHandle optimizeEncodings(ColumnFileHandle columnFileHandle)
            throws IOException
    {
        long shardId = columnFileHandle.getShardId();
        File shardPath = getShardPath(baseStorageDir, shardId);

        ImmutableList.Builder<BlockIterable> sourcesBuilder = ImmutableList.builder();
        ColumnFileHandle.Builder builder = ColumnFileHandle.builder(shardId);

        for (Map.Entry<ColumnHandle, File> entry : columnFileHandle.getFiles().entrySet()) {
            File file = entry.getValue();
            ColumnHandle columnHandle = entry.getKey();

            if (file.exists()) {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                checkState(file.length() == slice.length(), "File %s, length %s was mapped to Slice length %s", file.getAbsolutePath(), file.length(), slice.length());
                // Compute optimal encoding from stats
                BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
                BlocksFileStats stats = blocks.getStats();
                boolean rleEncode = stats.getAvgRunLength() > RUN_LENGTH_AVERAGE_CUTOFF;
                boolean dicEncode = stats.getUniqueCount() < DICTIONARY_CARDINALITY_CUTOFF;

                BlocksFileEncoding encoding = DEFAULT_ENCODING;

                if (ENABLE_OPTIMIZATION) {
                    if (dicEncode && rleEncode) {
                        encoding = BlocksFileEncoding.DIC_RLE;
                    }
                    else if (dicEncode) {
                        encoding = BlocksFileEncoding.DIC_RAW;
                    }
                    else if (rleEncode) {
                        encoding = BlocksFileEncoding.RLE;
                    }
                }

                File outputFile = getColumnFile(shardPath, columnHandle, encoding);
                Files.createParentDirs(outputFile);

                if (encoding == DEFAULT_ENCODING) {
                    // Optimization: source is already raw, so just move.
                    Files.move(file, outputFile);
                    // still register the file with the builder so that it can
                    // be committed correctly.
                    builder.addColumn(columnHandle, outputFile);
                }
                else {
                    // source builder and output builder move in parallel if the
                    // column gets written
                    sourcesBuilder.add(blocks);
                    builder.addColumn(columnHandle, outputFile, encoding);
                }
            }
            else {
                // fake file
                File outputFile = getColumnFile(shardPath, columnHandle, DEFAULT_ENCODING);
                builder.addColumn(columnHandle, outputFile);
            }
        }

        List<BlockIterable> sources = sourcesBuilder.build();
        ColumnFileHandle targetFileHandle = builder.build();

        if (!sources.isEmpty()) {
            AlignmentOperator source = new AlignmentOperator(sources);
            importData(source, targetFileHandle);
        }

        try {
            targetFileHandle.commit();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return targetFileHandle;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteStagingDirectory(ColumnFileHandle columnFileHandle)
    {
        File path = getShardPath(baseStagingDir, columnFileHandle.getShardId());

        while (path.delete() && !path.getParentFile().equals(baseStagingDir)) {
            path = path.getParentFile();
        }
    }

    private void importData(Operator source, ColumnFileHandle fileHandle)
    {
        for (PageIterator iterator = source.iterator(new OperatorStats()); iterator.hasNext();) {
            fileHandle.append(iterator.next());
        }
    }

    /**
     * Generate a file system path for a shard id. This creates a four level deep, two digit directory
     * where the least significant digits are the first level, the next significant digits are the second
     * and so on. Numbers that have more than eight digits are lumped together in the last level.
     *
     * <pre>
     *   1 --> 01/00/00/00
     *   1000 -> 00/10/00/00
     *   123456 -> 56/34/12/00
     *   4815162342 -> 42/23/16/4815
     * </pre>
     *
     * This ensures that files are spread out evenly through the tree while a path can still be easily navigated
     * by a human being.
     *
     * @param baseDir
     * @param shardId
     * @return
     */
    @VisibleForTesting
    static File getShardPath(File baseDir, long shardId)
    {
        Preconditions.checkArgument(shardId >= 0, "shardId must be >= 0");

        String value = format("%08d", shardId);
        int split = value.length() - 6;
        List<String> pathElements = ImmutableList.copyOf(Splitter.fixedLength(2).limit(3).split(value.substring(split)));
        String path = Joiner.on('/').join(Lists.reverse(pathElements)) + "/" + value.substring(0, split);
        return new File(baseDir, path);
    }

    private static File getColumnFile(File shardPath, ColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        checkState(columnHandle.getDataSourceType() == DataSourceType.NATIVE, "Can only import in a native column");
        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();
        return new File(shardPath, format("%s.%s.column", columnId, encoding.getName()));
    }

    private void commitShardColumns(final ColumnFileHandle columnFileHandle)
    {
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                StorageManagerDao dao = handle.attach(StorageManagerDao.class);

                for (Map.Entry<ColumnHandle, File> entry : columnFileHandle.getFiles().entrySet()) {
                    ColumnHandle columnHandle = entry.getKey();
                    File file = entry.getValue();

                    checkState(columnHandle.getDataSourceType() == DataSourceType.NATIVE, "Can only import in a native column");
                    long columnId = ((NativeColumnHandle) columnHandle).getColumnId();
                    String filename = file.getName();
                    dao.insertColumn(columnFileHandle.getShardId(), columnId, filename);
                }
            }
        });
    }

    @Override
    public BlockIterable getBlocks(long shardId, ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle);
        checkState(columnHandle.getDataSourceType() == DataSourceType.NATIVE, "Can only load blocks from a native column");
        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();

        checkState(shardExists(shardId), "shard %s has not yet been imported", shardId);
        String filename = dao.getColumnFilename(shardId, columnId);
        File file = new File(getShardPath(baseStorageDir, shardId), filename);

        // TODO: remove this hack when empty blocks are allowed
        if (!file.exists()) {
            return BlockUtils.emptyBlockIterable();
        }

        return convertFilesToBlocks(ImmutableList.of(file));
    }

    private BlockIterable convertFilesToBlocks(Iterable<File> files)
    {
        checkArgument(files.iterator().hasNext(), "no files in stream");

        Iterable<Block> blocks = Iterables.concat(Iterables.transform(files, new Function<File, Iterable<? extends Block>>()
        {
            @Override
            public Iterable<? extends Block> apply(File file)
            {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                return BlocksFileReader.readBlocks(slice);
            }
        }));

        return BlockUtils.toBlocks(blocks);
    }

    @Override
    public boolean shardExists(long shardId)
    {
        return dao.shardExists(shardId);
    }

    @Override
    public void dropShard(long shardId)
            throws IOException
    {
        // TODO: dropping needs to be globally coordinated with read queries
        List<String> shardFiles = dao.getShardFiles(shardId);
        for (String shardFile : shardFiles) {
            File file = new File(getShardPath(baseStorageDir, shardId), shardFile);
            java.nio.file.Files.deleteIfExists(file.toPath());
        }
        dao.dropShard(shardId);
    }

    private static File createDirectory(File dir)
            throws IOException
    {
        createDirectories(dir.toPath());
        return dir;
    }
}
