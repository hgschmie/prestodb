package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Predicates.in;

public class SubPlanBuilder
{
    private final PlanFragmentId id;
    private PlanNode root;
    private List<SubPlan> children = new ArrayList<>();
    private final Set<PlanNodeId> partitionedSources = new HashSet<>();

    private final SymbolAllocator allocator;

    public SubPlanBuilder(PlanFragmentId id, SymbolAllocator allocator, PlanNode root)
    {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(allocator, "allocator is null");
        Preconditions.checkNotNull(root, "root is null");

        this.allocator = allocator;
        this.id = id;
        this.root = root;
    }

    public PlanFragmentId getId()
    {
        return id;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public SubPlanBuilder setRoot(PlanNode root)
    {
        Preconditions.checkNotNull(root, "root is null");
        this.root = root;
        return this;
    }

    public boolean isPartitioned()
    {
        return !partitionedSources.isEmpty();
    }

    public Set<PlanNodeId> getPartitionedSources()
    {
        return partitionedSources;
    }

    public SubPlanBuilder addPartitionedSource(PlanNodeId partitionedSource)
    {
        this.partitionedSources.add(partitionedSource);
        return this;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    public SubPlanBuilder setChildren(Iterable<SubPlan> children)
    {
        this.children = Lists.newArrayList(children);
        return this;
    }

    public SubPlanBuilder addChild(SubPlan child)
    {
        this.children.add(child);
        return this;
    }

    public SubPlan build()
    {
        Set<Symbol> dependencies = SymbolExtractor.extract(root);

        PlanFragment fragment = new PlanFragment(id, partitionedSources, Maps.filterKeys(allocator.getTypes(), in(dependencies)), root);

        return new SubPlan(fragment, children);
    }
}
