package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.metadata.ColumnFileHandle;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.WritingSplit;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableWriterOperator
        implements SourceOperator, OutputProducingOperator<TableWriterResult>
{
    private final LocalStorageManager storageManager;
    private final String nodeIdentifier;
    private final Operator sourceOperator;
    private final List<Symbol> inputSymbols;
    private final Map<Symbol, ColumnHandle> columnHandles;

    private final AtomicBoolean used = new AtomicBoolean();

    private final AtomicReference<WritingSplit> input = new AtomicReference<>();

    private final AtomicReference<Set<TableWriterResult>> output = new AtomicReference<Set<TableWriterResult>>(ImmutableSet.<TableWriterResult>of());

    public TableWriterOperator(LocalStorageManager storageManager,
            String nodeIdentifier,
            List<Symbol> inputSymbols,
            Map<Symbol, ColumnHandle> columnHandles,
            Operator sourceOperator)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
        this.inputSymbols = checkNotNull(inputSymbols, "inputSymbols is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.sourceOperator = checkNotNull(sourceOperator, "sourceOperator is null");
    }

    @Override
    public void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkState(split.getDataSourceType() == DataSourceType.WRITING, "Non-writing split added!");
        checkState(input.get() == null, "Shard Id %s was already set!", input.get());
        input.set((WritingSplit) split);
    }

    @Override
    public void noMoreSplits()
    {
        checkState(input.get() != null, "No shard id was set!");
    }

    @Override
    public Set<TableWriterResult> getOutput()
    {
        return output.get();
    }

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return ImmutableList.of(SINGLE_LONG);
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        checkState(!used.getAndSet(true), "TableWriteOperator can be used only once");
        checkState(input.get() != null, "No shard id was set!");

        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();

        for (Symbol inputSymbol : inputSymbols) {
            ColumnHandle columnHandle = columnHandles.get(inputSymbol);
            checkState(columnHandle != null, "No column handle for %s found!", inputSymbol);
            columnHandlesBuilder.add(columnHandle);
        }

        checkState(sourceOperator.getChannelCount() == columnHandles.size(), "channel count does not match columnHandles list");

        try {
            ColumnFileHandle columnFileHandle = storageManager.createStagingFileHandles(input.get().getShardId(), columnHandlesBuilder.build());
            return new TableWriteIterator(sourceOperator.iterator(operatorStats), columnFileHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void commitFileHandle(ColumnFileHandle columnFileHandle)
    {
        try {
            storageManager.commit(columnFileHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public class TableWriteIterator
            extends AbstractPageIterator
    {
        private final ColumnFileHandle fileHandle;
        private final PageIterator sourceIterator;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TableWriteIterator(PageIterator sourceIterator, ColumnFileHandle fileHandle)
        {
            super(sourceIterator.getTupleInfos());
            this.sourceIterator = sourceIterator;
            this.fileHandle = fileHandle;
        }

        @Override
        protected void doClose()
        {
            if (!closed.getAndSet(true)) {
                commitFileHandle(fileHandle);
                sourceIterator.close();
                output.set(ImmutableSet.of(new TableWriterResult(input.get().getShardId(), nodeIdentifier)));
            }
        }

        @Override
        protected Page computeNext()
        {
            if (!sourceIterator.hasNext()) {
                return endOfData();
            }

            int rowCount = fileHandle.append(sourceIterator.next());

            // that should be simpler, right?
            Slice s = Slices.allocate(SINGLE_LONG.getFixedSize());
            SINGLE_LONG.setLong(s, 0, rowCount);
            SINGLE_LONG.setNotNull(s, 0);
            Block b = new UncompressedBlock(1, SINGLE_LONG, s);
            return new Page(b);
        }
    }
}
