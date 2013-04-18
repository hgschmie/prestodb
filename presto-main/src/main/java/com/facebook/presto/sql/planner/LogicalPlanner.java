package com.facebook.presto.sql.planner;

import com.facebook.presto.importer.PeriodicImportJob;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.AnalyzedDestination;
import com.facebook.presto.sql.analyzer.AnalyzedExpression;
import com.facebook.presto.sql.analyzer.AnalyzedFunction;
import com.facebook.presto.sql.analyzer.AnalyzedJoinClause;
import com.facebook.presto.sql.analyzer.AnalyzedOrdering;
import com.facebook.presto.sql.analyzer.AnalyzedWindow;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.storage.StorageManager;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createTable;
import static com.facebook.presto.sql.analyzer.AnalyzedFunction.argumentGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedFunction.windowExpressionGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedOrdering.expressionGetter;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;

public class LogicalPlanner
{
    private final Session session;
    private final Metadata metadata;
    private final PlanNodeIdAllocator idAllocator;
    private final List<PlanOptimizer> planOptimizers;
    private final PeriodicImportManager periodicImportManager;
    private final StorageManager storageManager;

    public LogicalPlanner(Session session,
            Metadata metadata,
            PeriodicImportManager periodicImportManager,
            StorageManager storageManager,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator)
    {
        this.session = checkNotNull(session, "session is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.planOptimizers = checkNotNull(planOptimizers, "planOptimizersFactory is null");
        this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
    }

    public PlanNode plan(AnalysisResult analysis)
    {
        PlanNode root = createOutputPlan(analysis);

        // make sure we produce a valid plan. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        Map<Symbol, Type> types = analysis.getTypes();

        for (PlanOptimizer optimizer : planOptimizers) {
            root = optimizer.optimize(root, session, types);
        }

        return root;
    }

    private PlanNode createOutputPlan(AnalysisResult analysis)
    {
        // TODO: This might just move into createQueryPlan to allow
        // multi stage destinations. Is that useful anyhow?
        PlanNode result;
        if (!analysis.getDestinations().isEmpty()) {
            result = createTableWriterPlan(analysis);
        }
        else {
            result = createQueryPlan(analysis);
        }

        List<String> names = new ArrayList<>();
        ImmutableMap.Builder<String, Symbol> assignments = ImmutableMap.builder();

        int i = 0;
        for (Field field : analysis.getOutputDescriptor().getFields()) {
            String name = field.getAttribute().orNull();
            while (name == null || names.contains(name)) {
                // TODO: this shouldn't be necessary once OutputNode uses Multimaps (requires updating to Jackson 2 for serialization support)
                i++;
                name = "_col" + i;
            }
            names.add(name);
            assignments.put(name, field.getSymbol());
        }

        return new OutputNode(idAllocator.getNextId(), result, names, assignments.build());
    }

    private PlanNode createQueryPlan(AnalysisResult analysis)
    {
        Query query = analysis.getRewrittenQuery();
        checkState(query != null, "query is null, can not create query plan!");
        PlanNode root = createRelationPlan(query.getFrom(), analysis);

        if (analysis.getPredicate() != null) {
            root = createFilterPlan(root, analysis.getPredicate());
        }

        Map<Expression, Symbol> substitutions = new HashMap<>();

        if (!analysis.getAggregations().isEmpty() || !analysis.getGroupByExpressions().isEmpty()) {
            root = createAggregatePlan(root,
                    ImmutableList.copyOf(analysis.getOutputExpressions().values()),
                    Lists.transform(analysis.getOrderBy(), expressionGetter()),
                    analysis.getAggregations(),
                    analysis.getGroupByExpressions(),
                    analysis.getSymbolAllocator(),
                    substitutions);
        }

        if (!analysis.getWindowFunctions().isEmpty()) {
            root = createWindowPlan(root,
                    ImmutableList.copyOf(analysis.getOutputExpressions().values()),
                    analysis.getWindowFunctions(),
                    analysis.getSymbolAllocator(),
                    substitutions);
        }

        if (analysis.isDistinct()) {
            root = createProjectPlan(root, analysis.getOutputExpressions(), substitutions); // project query outputs
            root = createDistinctPlan(root);
        }

        if (!analysis.getOrderBy().isEmpty()) {
            if (analysis.getLimit() != null) {
                root = createTopNPlan(root, analysis.getLimit(), analysis.getOrderBy(), analysis.getSymbolAllocator(), substitutions);
            }
            else {
                root = createSortPlan(root, analysis.getOrderBy(), analysis.getSymbolAllocator(), substitutions);
            }
        }

        if (!analysis.isDistinct()) {
            root = createProjectPlan(root, analysis.getOutputExpressions(), substitutions); // project query outputs
        }

        if (analysis.getLimit() != null && analysis.getOrderBy().isEmpty()) {
            root = createLimitPlan(root, analysis.getLimit());
        }

        return root;
    }

    private PlanNode createDistinctPlan(PlanNode source)
    {
        AggregationNode aggregation = new AggregationNode(idAllocator.getNextId(), source, source.getOutputSymbols(), ImmutableMap.<Symbol, FunctionCall>of(), ImmutableMap.<Symbol, FunctionHandle>of());
        return aggregation;
    }

    private PlanNode createLimitPlan(PlanNode source, long limit)
    {
        return new LimitNode(idAllocator.getNextId(), source, limit);
    }

    private PlanNode createTopNPlan(PlanNode source, long limit, List<AnalyzedOrdering> orderBy, SymbolAllocator allocator, Map<Expression, Symbol> substitutions)
    {
        /**
         * Turns SELECT $10, $11 ORDER BY expr($0, $1,...), expr($2, $3, ...) LIMIT c into
         *
         * - TopN [c] order by $4, $5
         *     - Project $4 = expr($0, $1, ...), $5 = expr($2, $3, ...), $10, $11
         */

        Map<Symbol, Expression> preProjectAssignments = new HashMap<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            // propagate all output symbols from underlying operator
            QualifiedNameReference expression = new QualifiedNameReference(symbol.toQualifiedName());
            preProjectAssignments.put(symbol, expression);
        }

        List<Symbol> orderBySymbols = new ArrayList<>();
        Map<Symbol, SortItem.Ordering> orderings = new HashMap<>();
        for (AnalyzedOrdering item : orderBy) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), item.getExpression().getRewrittenExpression());

            Symbol symbol = allocator.newSymbol(rewritten, item.getExpression().getType());

            orderBySymbols.add(symbol);
            preProjectAssignments.put(symbol, rewritten);
            orderings.put(symbol, item.getOrdering());
        }

        ProjectNode preProject = new ProjectNode(idAllocator.getNextId(), source, preProjectAssignments);
        return new TopNNode(idAllocator.getNextId(), preProject, limit, orderBySymbols, orderings);
    }

    private PlanNode createSortPlan(PlanNode source, List<AnalyzedOrdering> orderBy, SymbolAllocator allocator, Map<Expression, Symbol> substitutions)
    {
        Map<Symbol, Expression> preProjectAssignments = new HashMap<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            // propagate all output symbols from underlying operator
            QualifiedNameReference expression = new QualifiedNameReference(symbol.toQualifiedName());
            preProjectAssignments.put(symbol, expression);
        }

        List<Symbol> orderBySymbols = new ArrayList<>();
        Map<Symbol, SortItem.Ordering> orderings = new HashMap<>();
        for (AnalyzedOrdering item : orderBy) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), item.getExpression().getRewrittenExpression());

            Symbol symbol = allocator.newSymbol(rewritten, item.getExpression().getType());

            orderBySymbols.add(symbol);
            preProjectAssignments.put(symbol, rewritten);
            orderings.put(symbol, item.getOrdering());
        }

        ProjectNode preProject = new ProjectNode(idAllocator.getNextId(), source, preProjectAssignments);
        return new SortNode(idAllocator.getNextId(), preProject, orderBySymbols, orderings);
    }

    private PlanNode createWindowPlan(PlanNode source,
            List<AnalyzedExpression> outputs,
            Set<AnalyzedFunction> windowFunctions,
            SymbolAllocator allocator,
            Map<Expression, Symbol> outputSubstitutions)
    {
        // extract input expressions
        Set<AnalyzedExpression> inputs = ImmutableSet.copyOf(concat(
                IterableTransformer.on(windowFunctions)
                        .transformAndFlatten(argumentGetter())
                        .list(),
                IterableTransformer.on(windowFunctions)
                        .transformAndFlatten(windowExpressionGetter())
                        .list()));

        // assign a symbol for each input
        BiMap<Symbol, Expression> inputAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : inputs) {
            Symbol symbol = allocator.newSymbol(expression.getRewrittenExpression(), expression.getType());
            inputAssignments.put(symbol, expression.getRewrittenExpression());
        }

        // propagate all output symbols from underlying operator
        Map<Symbol, Expression> preProjections = new HashMap<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            QualifiedNameReference expression = new QualifiedNameReference(symbol.toQualifiedName());
            preProjections.put(symbol, expression);
        }

        // pre-project input symbols
        preProjections.putAll(inputAssignments);
        source = new ProjectNode(idAllocator.getNextId(), source, preProjections);

        // track window function outputs to rewrite in post-project
        Map<Expression, Symbol> substitutions = new HashMap<>();

        // add pre-projected symbols to substitution map
        for (Map.Entry<Symbol, Expression> entry : preProjections.entrySet()) {
            substitutions.put(entry.getValue(), entry.getKey());
        }

        // create a window node for each window function call
        for (AnalyzedFunction function : windowFunctions) {
            AnalyzedWindow window = function.getWindow().get();

            // map partition-by expressions
            List<Symbol> partitionBySymbols = new ArrayList<>();
            for (AnalyzedExpression item : window.getPartitionBy()) {
                Expression rewritten = TreeRewriter.rewriteWith(substitution(inputAssignments.inverse()), item.getRewrittenExpression());
                partitionBySymbols.add(allocator.newSymbol(rewritten, item.getType()));
            }

            // map order-by expressions
            List<Symbol> orderBySymbols = new ArrayList<>();
            Map<Symbol, SortItem.Ordering> orderings = new HashMap<>();
            for (AnalyzedOrdering item : window.getOrderBy()) {
                Expression rewritten = TreeRewriter.rewriteWith(substitution(inputAssignments.inverse()), item.getExpression().getRewrittenExpression());
                Symbol symbol = allocator.newSymbol(rewritten, item.getExpression().getType());
                orderBySymbols.add(symbol);
                orderings.put(symbol, item.getOrdering());
            }

            // build window function call map
            BiMap<Symbol, FunctionCall> functionAssignments = HashBiMap.create();
            Map<Symbol, FunctionHandle> functionHandles = new HashMap<>();

            // rewrite function call in terms of scalar inputs
            FunctionCall rewrittenFunction = TreeRewriter.rewriteWith(substitution(inputAssignments.inverse()), function.getRewrittenCall());
            Symbol symbol = allocator.newSymbol(function.getFunctionName().getSuffix(), function.getType());
            functionAssignments.put(symbol, rewrittenFunction);
            functionHandles.put(symbol, function.getFunctionInfo().getHandle());

            // build substitution map to rewrite assignments in post-project
            substitutions.put(function.getRewrittenCall(), symbol);

            // create window node
            source = new WindowNode(idAllocator.getNextId(), source, partitionBySymbols, orderBySymbols, orderings, functionAssignments, functionHandles);
        }

        // post-project scalar expressions based on window functions
        BiMap<Symbol, Expression> postProjectScalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : outputs) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), expression.getRewrittenExpression());
            Symbol symbol = allocator.newSymbol(rewritten, expression.getType());

            postProjectScalarAssignments.put(symbol, rewritten);

            // build substitution map to return to caller
            outputSubstitutions.put(expression.getRewrittenExpression(), symbol);
        }

        return new ProjectNode(idAllocator.getNextId(), source, postProjectScalarAssignments);
    }

    private PlanNode createAggregatePlan(PlanNode source,
            List<AnalyzedExpression> outputs,
            List<AnalyzedExpression> orderBy,
            Set<AnalyzedFunction> aggregations,
            List<AnalyzedExpression> groupBys,
            SymbolAllocator allocator,
            Map<Expression, Symbol> outputSubstitutions)
    {
        /**
         * Turns SELECT k1 + 1, sum(v1 * v2) - sum(v3 * v4) GROUP BY k1 + 1, k2 into
         *
         * 3. Project $0, $1, $7 = $5 - $6
         *   2. Aggregate by ($0, $1): $5 = sum($3), $6 = sum($4)
         *     1. Project $0 = k1 + 1, $1 = k2, $3 = v1 * v2, $4 = v3 * v4
         */

        // 1. Pre-project all scalar inputs
        Set<AnalyzedExpression> scalarExpressions = ImmutableSet.copyOf(concat(
                IterableTransformer.on(aggregations)
                        .transformAndFlatten(argumentGetter())
                        .list(),
                groupBys));

        BiMap<Symbol, Expression> scalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : scalarExpressions) {
            Symbol symbol = allocator.newSymbol(expression.getRewrittenExpression(), expression.getType());
            scalarAssignments.put(symbol, expression.getRewrittenExpression());
        }

        PlanNode preProjectNode = source;
        if (!scalarAssignments.isEmpty()) { // workaround to deal with COUNT's lack of inputs
            preProjectNode = new ProjectNode(idAllocator.getNextId(), source, scalarAssignments);
        }

        // 2. Aggregate
        Map<Expression, Symbol> substitutions = new HashMap<>();

        BiMap<Symbol, FunctionCall> aggregationAssignments = HashBiMap.create();
        Map<Symbol, FunctionHandle> functions = new HashMap<>();
        for (AnalyzedFunction aggregation : aggregations) {
            // rewrite function calls in terms of scalar inputs
            FunctionCall rewrittenFunction = TreeRewriter.rewriteWith(substitution(scalarAssignments.inverse()), aggregation.getRewrittenCall());
            Symbol symbol = allocator.newSymbol(aggregation.getFunctionName().getSuffix(), aggregation.getType());
            aggregationAssignments.put(symbol, rewrittenFunction);
            functions.put(symbol, aggregation.getFunctionInfo().getHandle());

            // build substitution map to rewrite assignments in post-project
            substitutions.put(aggregation.getRewrittenCall(), symbol);
        }

        List<Symbol> groupBySymbols = new ArrayList<>();
        for (AnalyzedExpression groupBy : groupBys) {
            Symbol symbol = scalarAssignments.inverse().get(groupBy.getRewrittenExpression());

            substitutions.put(groupBy.getRewrittenExpression(), symbol);
            groupBySymbols.add(symbol);
        }

        PlanNode aggregationNode = new AggregationNode(idAllocator.getNextId(), preProjectNode, groupBySymbols, aggregationAssignments, functions);

        // 3. Post-project scalar expressions based on aggregations
        BiMap<Symbol, Expression> postProjectScalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : ImmutableSet.copyOf(concat(outputs, groupBys, orderBy))) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), expression.getRewrittenExpression());
            Symbol symbol = allocator.newSymbol(rewritten, expression.getType());

            postProjectScalarAssignments.put(symbol, rewritten);

            // build substitution map to return to caller
            outputSubstitutions.put(expression.getRewrittenExpression(), symbol);
        }

        return new ProjectNode(idAllocator.getNextId(), aggregationNode, postProjectScalarAssignments);
    }

    private PlanNode createProjectPlan(PlanNode root, Map<Symbol, AnalyzedExpression> outputAnalysis, final Map<Expression, Symbol> substitutions)
    {
        Map<Symbol, Expression> outputs = Maps.transformValues(outputAnalysis, new Function<AnalyzedExpression, Expression>()
        {
            @Override
            public Expression apply(AnalyzedExpression input)
            {
                return TreeRewriter.rewriteWith(substitution(substitutions), input.getRewrittenExpression());
            }
        });

        return new ProjectNode(idAllocator.getNextId(), root, outputs);
    }

    private FilterNode createFilterPlan(PlanNode source, AnalyzedExpression predicate)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate.getRewrittenExpression());
    }

    private PlanNode createRelationPlan(List<Relation> relations, AnalysisResult analysis)
    {
        Relation relation = Iterables.getOnlyElement(relations); // TODO: add join support

        if (relation instanceof Table) {
            return createScanNode((Table) relation, analysis);
        }
        else if (relation instanceof AliasedRelation) {
            return createRelationPlan(ImmutableList.of(((AliasedRelation) relation).getRelation()), analysis);
        }
        else if (relation instanceof Subquery) {
            AnalysisResult subqueryAnalysis = analysis.getAnalysis((Subquery) relation);
            return createQueryPlan(subqueryAnalysis);
        }
        else if (relation instanceof Join) {
            return createJoinPlan((Join) relation, analysis);
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private PlanNode createJoinPlan(Join join, AnalysisResult analysis)
    {
        PlanNode leftPlan = createRelationPlan(ImmutableList.of(join.getLeft()), analysis);
        PlanNode rightPlan = createRelationPlan(ImmutableList.of(join.getRight()), analysis);

        // We insert a projection on the left side and right side blindly -- they'll get optimized out later if not needed
        Map<Symbol, Expression> leftProjections = new HashMap<>();
        Map<Symbol, Expression> rightProjections = new HashMap<>();

        // add a pass-through projection for the outputs of left and right
        for (Symbol symbol : leftPlan.getOutputSymbols()) {
            leftProjections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
        }
        for (Symbol symbol : rightPlan.getOutputSymbols()) {
            rightProjections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
        }

        // next, handle the join clause...
        List<AnalyzedJoinClause> criteria = analysis.getJoinCriteria(join);

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiJoinClauses = ImmutableList.builder();
        for (AnalyzedJoinClause analyzedClause : criteria) {
            // insert a projection for the sub-expression corresponding to the left side and assign it to
            // a new symbol. If the expression is already a simple symbol reference, this will result in an identity projection
            AnalyzedExpression left = analyzedClause.getLeft();
            Symbol leftSymbol = analysis.getSymbolAllocator().newSymbol(left.getRewrittenExpression(), left.getType());
            leftProjections.put(leftSymbol, left.getRewrittenExpression());

            // do the same for the right side
            AnalyzedExpression right = analyzedClause.getRight();
            Symbol rightSymbol = analysis.getSymbolAllocator().newSymbol(right.getRewrittenExpression(), right.getType());
            rightProjections.put(rightSymbol, right.getRewrittenExpression());

            equiJoinClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
        }

        leftPlan = new ProjectNode(idAllocator.getNextId(), leftPlan, leftProjections);
        rightPlan = new ProjectNode(idAllocator.getNextId(), rightPlan, rightProjections);

        return new JoinNode(idAllocator.getNextId(), leftPlan, rightPlan, equiJoinClauses.build());
    }

    private PlanNode createScanNode(Table table, AnalysisResult analysis)
    {
        TupleDescriptor descriptor = analysis.getTableDescriptor(table);
        TableMetadata metadata = analysis.getTableMetadata(table);

        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();

        List<Field> fields = descriptor.getFields();
        for (Field field : fields) {
            columns.put(field.getSymbol(), field.getColumn().get());
        }

        return new TableScanNode(idAllocator.getNextId(), metadata.getTableHandle().get(), columns.build());
    }

    private PlanNode createTableWriterPlan(AnalysisResult analysis)
    {
        checkState(analysis.getDestinations().size() == 1, "only a single table destination is currently supported");
        AnalyzedDestination destination = Iterables.getOnlyElement(analysis.getDestinations());

        // Build the plan for the attached query node
        AnalysisResult queryAnalysis = analysis.getAnalysis(destination);
        PlanNode queryNode = createQueryPlan(queryAnalysis);

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (Field field : queryAnalysis.getOutputDescriptor().getFields()) {
            ColumnMetadata columnMetadata = new ColumnMetadata(field.getAttribute().get(), field.getType().getRawType());
            columns.add(columnMetadata);
        }

        TableMetadata dstTableMetadata = createTable(metadata, destination.getTableName(), columns.build());
        checkState(dstTableMetadata.getTableHandle().isPresent(), "can not import into a table without table handle");
        checkState(dstTableMetadata.getTableHandle().get().getDataSourceType() == DataSourceType.NATIVE, "can not import into non-native table %s", dstTableMetadata.getTable());
        QualifiedTableName srcTableName = getTableNameFromQuery(session, queryAnalysis);

        storageManager.insertSourceTable(((NativeTableHandle) dstTableMetadata.getTableHandle().get()), srcTableName);

        // if a refresh is present, create a periodic import for this table
        if (destination.getRefresh().isPresent()) {
            int importInterval = Integer.parseInt(destination.getRefresh().get());
            checkState(importInterval > 0, "import interval must be > 0");
            PeriodicImportJob job = PeriodicImportJob.createJob(srcTableName, dstTableMetadata.getTable(), importInterval);
            periodicImportManager.insertJob(job);
        }

        ImmutableMap.Builder<Symbol, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Type> inputTypesBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> inputSymbolsBuilder = ImmutableList.builder();

        for (Field field : queryAnalysis.getOutputDescriptor().getFields()) {
            inputTypesBuilder.put(field.getSymbol(), field.getType());
            inputSymbolsBuilder.add(field.getSymbol());

            ColumnHandle columnHandle = findColumnHandle(field, dstTableMetadata);
            checkState(columnHandle != null, "Could not match symbol %s to any table column!", field.getSymbol());
            columnHandlesBuilder.put(field.getSymbol(), columnHandle);
        }

        ImmutableMap.Builder<Symbol, Type> outputTypesBuilder = ImmutableMap.builder();
        for (Field field : analysis.getOutputDescriptor().getFields()) {
            outputTypesBuilder.put(field.getSymbol(), field.getType());
        }

        TableWriterNode writerNode = new TableWriterNode(idAllocator.getNextId(),
                queryNode,
                dstTableMetadata.getTableHandle().get(),
                inputSymbolsBuilder.build(),
                inputTypesBuilder.build(),
                columnHandlesBuilder.build(),
                outputTypesBuilder.build());

        Map<Symbol, AnalyzedExpression> outputExpressions = analysis.getOutputExpressions();
        checkState(outputExpressions.size() == 1, "only a single output symbol is supported");
        Symbol outputSymbol = Iterables.getOnlyElement(outputExpressions.keySet());

        // Put a simple SUM(<output symbol>) on top of the table writer node
        // Build the sum function info here, we need it later in the planner
        FunctionInfo sum = metadata.getFunction(QualifiedName.of("sum"), Lists.transform(ImmutableList.of(Type.LONG), Type.toRaw()));
        FunctionCall sumCall = new FunctionCall(QualifiedName.of(outputSymbol.getName()), ImmutableList.of(nameReference(outputSymbol.getName())));
        PlanNode aggregationNode = new AggregationNode(idAllocator.getNextId(), writerNode, ImmutableList.<Symbol>of(), ImmutableMap.of(outputSymbol, sumCall), ImmutableMap.of(outputSymbol, sum.getHandle()));

        return aggregationNode;
    }

    private ColumnHandle findColumnHandle(Field field, TableMetadata tableMetadata)
    {
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            checkState(columnMetadata.getColumnHandle().isPresent(), "can not import into a column without column handle");

            if (columnMetadata.getName().equals(field.getSymbol().getName())) {
                return columnMetadata.getColumnHandle().get();
            }
        }

        return null;
    }

    private QualifiedTableName getTableNameFromQuery(Session session, AnalysisResult queryAnalysis)
    {
        // Yup. Total hack.
        Query q = queryAnalysis.getRewrittenQuery();
        List<Relation> relations = q.getFrom();
        checkState(relations.size() == 1, "query uses more than one table");
        Relation r = Iterables.getOnlyElement(relations);
        checkState(r instanceof Table, "query does not select from a table");
        return MetadataUtil.createQualifiedTableName(session, ((Table) r).getName());
    }

    private static NodeRewriter<Void> substitution(final Map<Expression, Symbol> substitutions)
    {
        return new NodeRewriter<Void>()
        {
            @Override
            public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
            {
                Symbol symbol = substitutions.get(node);
                if (symbol != null) {
                    return new QualifiedNameReference(symbol.toQualifiedName());
                }

                return treeRewriter.defaultRewrite(node, context);
            }
        };
    }
}
