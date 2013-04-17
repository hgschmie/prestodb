package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.QualifiedTableName;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class AnalyzedDestination
{
    private final QualifiedTableName tableName;
    private final Optional<String> refresh;

    public AnalyzedDestination(QualifiedTableName tableName, Optional<String> refresh)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.refresh = checkNotNull(refresh, "refresh is null");
    }

    public QualifiedTableName getTableName()
    {
        return tableName;
    }

    public Optional<String> getRefresh()
    {
        return refresh;
    }
}
