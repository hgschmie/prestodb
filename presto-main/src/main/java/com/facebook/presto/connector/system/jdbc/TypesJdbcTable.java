/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.connector.system.jdbc;

import com.facebook.presto.spi.ColumnName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import javax.inject.Inject;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Collection;

import static com.facebook.presto.connector.system.jdbc.ColumnJdbcTable.columnSize;
import static com.facebook.presto.connector.system.jdbc.ColumnJdbcTable.jdbcDataType;
import static com.facebook.presto.connector.system.jdbc.ColumnJdbcTable.numPrecRadix;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.ColumnName.*;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class TypesJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "types");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column(createColumnName("type_name"), createUnboundedVarcharType())
            .column(createColumnName("data_type"), BIGINT)
            .column(createColumnName("precision"), BIGINT)
            .column(createColumnName("literal_prefix"), createUnboundedVarcharType())
            .column(createColumnName("literal_suffix"), createUnboundedVarcharType())
            .column(createColumnName("create_params"), createUnboundedVarcharType())
            .column(createColumnName("nullable"), BIGINT)
            .column(createColumnName("case_sensitive"), BOOLEAN)
            .column(createColumnName("searchable"), BIGINT)
            .column(createColumnName("unsigned_attribute"), BOOLEAN)
            .column(createColumnName("fixed_prec_scale"), BOOLEAN)
            .column(createColumnName("auto_increment"), BOOLEAN)
            .column(createColumnName("local_type_name"), createUnboundedVarcharType())
            .column(createColumnName("minimum_scale"), BIGINT)
            .column(createColumnName("maximum_scale"), BIGINT)
            .column(createColumnName("sql_data_type"), BIGINT)
            .column(createColumnName("sql_datetime_sub"), BIGINT)
            .column(createColumnName("num_prec_radix"), BIGINT)
            .build();

    private final TypeManager typeManager;

    @Inject
    public TypesJdbcTable(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(METADATA);
        for (Type type : typeManager.getTypes()) {
            addTypeRow(table, type);
        }
        addParametricTypeRows(table, typeManager.getParametricTypes());
        return table.build().cursor();
    }

    private static void addTypeRow(Builder builder, Type type)
    {
        builder.addRow(
                type.getDisplayName(),
                jdbcDataType(type),
                columnSize(type),
                null,
                null,
                null,
                DatabaseMetaData.typeNullable,
                false,
                type.isComparable() ? DatabaseMetaData.typeSearchable : DatabaseMetaData.typePredNone,
                null,
                false,
                null,
                null,
                0,
                0,
                null,
                null,
                numPrecRadix(type));
    }

    private static void addParametricTypeRows(Builder builder, Collection<ParametricType> types)
    {
        for (ParametricType type : types) {
            String typeName = type.getName();
            builder.addRow(
                    typeName,
                    typeName.equalsIgnoreCase("array") ? Types.ARRAY : Types.JAVA_OBJECT,
                    null,
                    null,
                    null,
                    null,
                    DatabaseMetaData.typeNullable,
                    false,
                    DatabaseMetaData.typePredNone,
                    null,
                    false,
                    null,
                    null,
                    0,
                    0,
                    null,
                    null,
                    null);
        }
    }
}
