package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.InMemoryOrderByOperator;
import com.facebook.presto.operator.InMemoryWindowOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OutputProducingOperator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.SourceHashProvider;
import com.facebook.presto.operator.SourceHashProviderFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.operator.TopNOperator;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.server.ExchangeOperatorFactory;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.tree.Input.fieldGetter;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;

public class LocalExecutionPlanner
{
    private final Session session;
    private final NodeInfo nodeInfo;
    private final Metadata metadata;
    private final Map<Symbol, Type> types;

    private final OperatorStats operatorStats;

    private final SourceHashProviderFactory joinHashFactory;
    private final DataSize maxOperatorMemoryUsage;

    private final DataStreamProvider dataStreamProvider;
    private final StorageManager storageManager;
    private final ExchangeOperatorFactory exchangeOperatorFactory;

    public LocalExecutionPlanner(Session session,
            NodeInfo nodeInfo,
            Metadata metadata,
            Map<Symbol, Type> types,
            OperatorStats operatorStats,
            SourceHashProviderFactory joinHashFactory,
            DataSize maxOperatorMemoryUsage,
            DataStreamProvider dataStreamProvider,
            StorageManager storageManager,
            ExchangeOperatorFactory exchangeOperatorFactory)
    {
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeOperatorFactory = exchangeOperatorFactory;
        this.session = checkNotNull(session, "session is null");
        this.operatorStats = Preconditions.checkNotNull(operatorStats, "operatorStats is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.types = checkNotNull(types, "types is null");
        this.joinHashFactory = checkNotNull(joinHashFactory, "joinHashFactory is null");
        this.maxOperatorMemoryUsage = Preconditions.checkNotNull(maxOperatorMemoryUsage, "maxOperatorMemoryUsage is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    public LocalExecutionPlan plan(PlanNode plan)
    {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext();
        Operator rootOperator = plan.accept(new Visitor(), context).getOperator();
        return new LocalExecutionPlan(rootOperator, context.getSourceOperators(), context.getOutputOperators());
    }

    private static class LocalExecutionPlanContext
    {
        private final Map<PlanNodeId, SourceOperator> sourceOperators = new HashMap<>();
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators = new HashMap<>();

        private void addSourceOperator(PlanNode node, SourceOperator sourceOperator)
        {
            checkState(this.sourceOperators.put(node.getId(), sourceOperator) == null, "Node %s already had a source operator assigned!", node);
        }

        private void addOutputOperator(PlanNode node, OutputProducingOperator<?> outputOperator)
        {
            checkState(this.outputOperators.put(node.getId(), outputOperator) == null, "Node %s already had a source operator assigned!", node);
        }

        private Map<PlanNodeId, SourceOperator> getSourceOperators()
        {
            return ImmutableMap.copyOf(sourceOperators);
        }
        private Map<PlanNodeId, OutputProducingOperator<?>> getOutputOperators()
        {
            return ImmutableMap.copyOf(outputOperators);
        }
    }

    public static class LocalExecutionPlan
    {
        private final Operator rootOperator;
        private final Map<PlanNodeId, SourceOperator> sourceOperators;
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators;

        public LocalExecutionPlan(Operator rootOperator,
                Map<PlanNodeId, SourceOperator> sourceOperators,
                Map<PlanNodeId, OutputProducingOperator<?>> outputOperators)
        {
            this.rootOperator = checkNotNull(rootOperator, "rootOperator is null");
            this.sourceOperators = ImmutableMap.copyOf(checkNotNull(sourceOperators, "sourceOperators is null"));
            this.outputOperators = ImmutableMap.copyOf(checkNotNull(outputOperators, "outputOperators is null"));
        }

        public Operator getRootOperator()
        {
            return rootOperator;
        }

        public Map<PlanNodeId, SourceOperator> getSourceOperators()
        {
            return sourceOperators;
        }

        public Map<PlanNodeId, OutputProducingOperator<?>> getOutputOperators()
        {
            return outputOperators;
        }
}

    private class Visitor
            extends PlanVisitor<LocalExecutionPlanContext, PhysicalOperation>
    {
        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            List<TupleInfo> tupleInfo = getSourceOperatorTupleInfos(node);

            SourceOperator operator = exchangeOperatorFactory.createExchangeOperator(tupleInfo);
            context.addSourceOperator(node, operator);

            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            Map<Symbol, Input> outputMappings = new HashMap<>();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel, 0)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(operator, outputMappings);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> resultSymbols = Lists.transform(node.getColumnNames(), forMap(node.getAssignments()));

            // see if we need to introduce a projection
            //   1. verify that there's one symbol per channel
            //   2. verify that symbols from "source" match the expected order of columns according to OutputNode
            Ordering<Input> comparator = inputOrdering();

            List<Symbol> sourceSymbols = IterableTransformer.on(source.getLayout().entrySet())
                    .orderBy(comparator.onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list();

            if (resultSymbols.equals(sourceSymbols) && resultSymbols.size() == source.getOperator().getChannelCount()) {
                // no projection needed
                return source;
            }

            // otherwise, introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(resultSymbols, source.getLayout(), types);

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Symbol> orderBySymbols = node.getOrderBy();

            // sort by PARTITION BY, then by ORDER BY
            List<Symbol> orderingSymbols = ImmutableList.copyOf(Iterables.concat(partitionBySymbols, orderBySymbols));

            // insert a projection to put all the sort fields in a single channel if necessary
            if (!orderingSymbols.isEmpty()) {
                source = packIfNecessary(orderingSymbols, source);
            }

            // find channel that fields were packed into if there is an ordering
            int orderByChannel = 0;
            if (!orderBySymbols.isEmpty()) {
                orderByChannel = Iterables.getOnlyElement(getChannelsForSymbols(orderingSymbols, source.getLayout()));
            }

            int[] partitionFields = new int[partitionBySymbols.size()];
            for (int i = 0; i < partitionFields.length; i++) {
                Symbol symbol = partitionBySymbols.get(i);
                partitionFields[i] = source.getLayout().get(symbol).getField();
            }

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);
                sortFields[i] = source.getLayout().get(symbol).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }

            int[] outputChannels = new int[source.getOperator().getChannelCount()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            ImmutableList.Builder<WindowFunction> windowFunctions = ImmutableList.builder();
            List<Symbol> windowFunctionOutputSymbols = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();
                FunctionHandle handle = node.getFunctionHandles().get(symbol);
                windowFunctions.add(metadata.getFunction(handle).getWindowFunction().get());
                windowFunctionOutputSymbols.add(symbol);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getOperator().getChannelCount();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            Operator operator = new InMemoryWindowOperator(
                    source.getOperator(),
                    orderByChannel,
                    outputChannels,
                    windowFunctions.build(),
                    partitionFields,
                    sortFields,
                    sortOrder,
                    1_000_000,
                    maxOperatorMemoryUsage);

            return new PhysicalOperation(operator, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            Preconditions.checkArgument(orderBySymbols.size() == 1, "ORDER BY multiple fields + LIMIT not yet supported"); // TODO

            Symbol orderBySymbol = Iterables.getOnlyElement(orderBySymbols);
            int keyChannel = source.getLayout().get(orderBySymbol).getChannel();

            Ordering<TupleReadable> ordering = Ordering.from(FieldOrderedTupleComparator.INSTANCE);
            if (node.getOrderings().get(orderBySymbol) == SortItem.Ordering.ASCENDING) {
                ordering = ordering.reverse();
            }

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), types);

            TopNOperator operator = new TopNOperator(source.getOperator(), (int) node.getCount(), keyChannel, mappings.getProjections(), ordering);
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            // insert a projection to put all the sort fields in a single channel if necessary
            source = packIfNecessary(orderBySymbols, source);

            int orderByChannel = Iterables.getOnlyElement(getChannelsForSymbols(orderBySymbols, source.getLayout()));

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);

                sortFields[i] = source.getLayout().get(symbol).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }

            int[] outputChannels = new int[source.getOperator().getChannelCount()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            Operator operator = new InMemoryOrderByOperator(source.getOperator(), orderByChannel, outputChannels, 1_000_000, sortFields, sortOrder, maxOperatorMemoryUsage);
            return new PhysicalOperation(operator, source.getLayout());
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            return new PhysicalOperation(new LimitOperator(source.getOperator(), node.getCount()), source.getLayout());
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupBy().isEmpty()) {
                return planGlobalAggregation(node, source);
            }

            return planGroupByAggregation(node, source);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            FilterFunction filter = new InterpretedFilterFunction(node.getPredicate(), source.getLayout(), metadata, session);

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), types);

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), filter, mappings.getProjections());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Map<Symbol, Input> outputMappings = new HashMap<>();
            List<ProjectionFunction> projections = new ArrayList<>();
            for (int i = 0; i < node.getExpressions().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                Expression expression = node.getExpressions().get(i);

                ProjectionFunction function;
                if (expression instanceof QualifiedNameReference) {
                    // fast path when we know it's a direct symbol reference
                    Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                    function = ProjectionFunctions.singleColumn(types.get(reference).getRawType(), source.getLayout().get(symbol));
                }
                else {
                    function = new InterpretedProjectionFunction(types.get(symbol), expression, source.getLayout(), metadata, session);
                }
                projections.add(function);

                outputMappings.put(symbol, new Input(i, 0)); // one field per channel
            }

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, projections);
            return new PhysicalOperation(operator, outputMappings);
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            Map<Symbol, Input> mappings = new HashMap<>();
            List<ColumnHandle> columns = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));

                mappings.put(symbol, new Input(channel, 0)); // one column per channel
                channel++;
            }

            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node);
            TableScanOperator operator = new TableScanOperator(dataStreamProvider, tupleInfos, columns);
            context.addSourceOperator(node, operator);
            return new PhysicalOperation(operator, mappings);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // introduce a projection to put all fields from the left side into a single channel if necessary
            PhysicalOperation leftSource = node.getLeft().accept(this, context);
            List<Symbol> leftSymbols = Lists.transform(clauses, leftGetter());
            leftSource = packIfNecessary(leftSymbols, leftSource);

            // do the same on the right side
            PhysicalOperation rightSource = node.getRight().accept(this, context);
            List<Symbol> rightSymbols = Lists.transform(clauses, rightGetter());
            rightSource = packIfNecessary(rightSymbols, rightSource);

            int probeChannel = Iterables.getOnlyElement(getChannelsForSymbols(leftSymbols, leftSource.getLayout()));
            int buildChannel = Iterables.getOnlyElement(getChannelsForSymbols(rightSymbols, rightSource.getLayout()));

            SourceHashProvider hashProvider = joinHashFactory.getSourceHashProvider(node, rightSource.getOperator(), buildChannel, operatorStats);

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(leftSource.getLayout());

            // inputs from right side of the join are laid out following the input from the left side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = leftSource.getOperator().getChannelCount();
            for (Map.Entry<Symbol, Input> entry : rightSource.getLayout().entrySet()) {
                Input input = entry.getValue();
                outputMappings.put(entry.getKey(), new Input(offset + input.getChannel(), input.getField()));
            }

            HashJoinOperator operator = new HashJoinOperator(hashProvider, leftSource.getOperator(), probeChannel);
            return new PhysicalOperation(operator, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitSink(SinkNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // if any symbols are mapped to a non-zero field, re-map to one field per channel
            // TODO: this is currently what the exchange operator expects -- figure out how to remove this assumption
            // to avoid unnecessary projections
            boolean needsProjection = IterableTransformer.on(source.getLayout().values())
                    .transform(fieldGetter())
                    .any(not(equalTo(0)));

            if (needsProjection) {
                return projectToOneFieldPerChannel(source, types);
            }

            return source;
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            LocalExecutionPlanner queryPlanner = new LocalExecutionPlanner(session,
                    nodeInfo,
                    metadata,
                    node.getInputTypes(),
                    operatorStats,
                    joinHashFactory,
                    maxOperatorMemoryUsage,
                    dataStreamProvider,
                    storageManager,
                    exchangeOperatorFactory);

            PhysicalOperation query = node.getSource().accept(queryPlanner.new Visitor(), context);

            // introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(node.getInputSymbols(), query.getLayout(), node.getInputTypes());
            Operator sourceOperator = new FilterAndProjectOperator(query.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());

            Symbol outputSymbol = Iterables.getOnlyElement(node.getOutputTypes().keySet());

            TableWriterOperator operator = new TableWriterOperator(storageManager,
                    nodeInfo.getNodeId(),
                    node.getInputSymbols(),
                    node.getColumnHandles(),
                    sourceOperator);

            context.addSourceOperator(node, operator);
            context.addOutputOperator(node, operator);

            return new PhysicalOperation(operator,
                    ImmutableMap.of(outputSymbol, new Input(0, 0)));
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<TupleInfo> getSourceOperatorTupleInfos(PlanNode node)
        {
            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            return ImmutableList.copyOf(IterableTransformer.on(node.getOutputSymbols())
                    .transform(Functions.forMap(types))
                    .transform(Type.toRaw())
                    .transform(new Function<TupleInfo.Type, TupleInfo>()
                    {
                        @Override
                        public TupleInfo apply(TupleInfo.Type input)
                        {
                            return new TupleInfo(input);
                        }
                    })
                    .list());
        }

        private AggregationFunctionDefinition buildFunctionDefinition(PhysicalOperation source, FunctionHandle function, FunctionCall call)
        {
            List<Input> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(source.getLayout().get(argumentSymbol));
            }

            return metadata.getFunction(function).bind(arguments);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                outputMappings.put(symbol, new Input(outputChannel, 0)); // one aggregation per channel
                outputChannel++;
            }

            Operator operator = new AggregationOperator(source.getOperator(), node.getStep(), functionDefinitions);
            return new PhysicalOperation(operator, outputMappings.build());
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            // introduce a projection to put all group by fields from the source into a single channel if necessary
            source = packIfNecessary(groupBySymbols, source);

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            // add group-by key fields. They all go in channel 0 in the same order produced by the source operator
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, new Input(0, source.getLayout().get(symbol).getField()));
            }

            // aggregations go in remaining channels starting at 1, one per channel
            int channel = 1;
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            Operator aggregationOperator = new HashAggregationOperator(source.getOperator(),
                    Iterables.getOnlyElement(getChannelsForSymbols(groupBySymbols, source.getLayout())),
                    node.getStep(),
                    functionDefinitions,
                    100_000,
                    maxOperatorMemoryUsage);

            return new PhysicalOperation(aggregationOperator, outputMappings.build());
        }
    }

    private static IdentityProjectionInfo computeIdentityMapping(List<Symbol> symbols, Map<Symbol, Input> inputLayout, Map<Symbol, Type> types)
    {
        Map<Symbol, Input> outputLayout = new HashMap<>();
        List<ProjectionFunction> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), inputLayout.get(symbol));
            projections.add(function);
            outputLayout.put(symbol, new Input(channel, 0)); // one field per channel
            channel++;
        }

        return new IdentityProjectionInfo(outputLayout, projections);
    }

    /**
     * Inserts a projection if the provided symbols are not in a single channel by themselves
     */
    private PhysicalOperation packIfNecessary(List<Symbol> symbols, PhysicalOperation source)
    {
        Set<Integer> channels = getChannelsForSymbols(symbols, source.getLayout());
        List<TupleInfo> tupleInfos = source.getOperator().getTupleInfos();
        if (channels.size() > 1 || tupleInfos.get(Iterables.getOnlyElement(channels)).getFieldCount() > 1) {
            source = pack(source, symbols, types);
        }
        return source;
    }

    /**
     * Inserts a Projection that places the requested symbols into the same channel in the order specified
     */
    private static PhysicalOperation pack(PhysicalOperation source, List<Symbol> symbols, Map<Symbol, Type> types)
    {
        Preconditions.checkArgument(!symbols.isEmpty(), "symbols is empty");

        List<Symbol> otherSymbols = ImmutableList.copyOf(Sets.difference(source.getLayout().keySet(), ImmutableSet.copyOf(symbols)));

        // split composite channels into one field per channel. TODO: Fix it so that it preserves the layout of channels for "otherSymbols"
        IdentityProjectionInfo mappings = computeIdentityMapping(otherSymbols, source.getLayout(), types);

        ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
        ImmutableList.Builder<ProjectionFunction> projections = ImmutableList.builder();

        outputMappings.putAll(mappings.getOutputLayout());
        projections.addAll(mappings.getProjections());

        // append a projection that packs all the input symbols into a single channel (it goes in the last channel)
        List<ProjectionFunction> packedProjections = new ArrayList<>();
        int channel = mappings.getProjections().size();
        int field = 0;
        for (Symbol symbol : symbols) {
            packedProjections.add(ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), source.getLayout().get(symbol)));
            outputMappings.put(symbol, new Input(channel, field));
            field++;
        }
        projections.add(ProjectionFunctions.concat(packedProjections));

        Operator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, projections.build());
        return new PhysicalOperation(operator, outputMappings.build());
    }

    private static PhysicalOperation projectToOneFieldPerChannel(PhysicalOperation source, Map<Symbol, Type> types)
    {
        List<Symbol> symbols = ImmutableList.copyOf(source.getLayout().keySet());
        IdentityProjectionInfo mappings = computeIdentityMapping(symbols, source.getLayout(), types);

        Operator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
        return new PhysicalOperation(operator, mappings.getOutputLayout());
    }

    private static Set<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Input> layout)
    {
        return IterableTransformer.on(symbols)
                .transform(Functions.forMap(layout))
                .transform(Input.channelGetter())
                .set();
    }

    private static class IdentityProjectionInfo
    {
        private Map<Symbol, Input> layout = new HashMap<>();
        private List<ProjectionFunction> projections = new ArrayList<>();

        public IdentityProjectionInfo(Map<Symbol, Input> outputLayout, List<ProjectionFunction> projections)
        {
            this.layout = outputLayout;
            this.projections = projections;
        }

        public Map<Symbol, Input> getOutputLayout()
        {
            return layout;
        }

        public List<ProjectionFunction> getProjections()
        {
            return projections;
        }
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private Operator operator;
        private Map<Symbol, Input> layout;

        public PhysicalOperation(Operator operator, Map<Symbol, Input> layout)
        {
            checkNotNull(operator, "operator is null");
            Preconditions.checkNotNull(layout, "layout is null");

            this.operator = operator;
            this.layout = layout;
        }

        public Operator getOperator()
        {
            return operator;
        }

        public Map<Symbol, Input> getLayout()
        {
            return layout;
        }
    }

    private static Ordering<Input> inputOrdering()
    {
        return Ordering.from(new Comparator<Input>() {
            @Override
            public int compare(Input o1, Input o2)
            {
                return ComparisonChain.start()
                        .compare(o1.getChannel(), o2.getChannel())
                        .compare(o1.getField(), o2.getField())
                        .result();
            }
        });
    }
}
