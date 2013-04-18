package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeSplit.class, name = "native"),
        @JsonSubTypes.Type(value = InternalSplit.class, name = "internal"),
        @JsonSubTypes.Type(value = ImportSplit.class, name = "import"),
        @JsonSubTypes.Type(value = RemoteSplit.class, name = "remote"),
        @JsonSubTypes.Type(value = WritingSplit.class, name = "writing")})
public interface Split
{
    DataSourceType getDataSourceType();

    Object getInfo();
}
