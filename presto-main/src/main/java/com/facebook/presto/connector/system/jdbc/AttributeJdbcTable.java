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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.ColumnName.*;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class AttributeJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "attributes");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column(createColumnName("type_cat"), createUnboundedVarcharType())
            .column(createColumnName("type_schem"), createUnboundedVarcharType())
            .column(createColumnName("type_name"), createUnboundedVarcharType())
            .column(createColumnName("attr_name"), createUnboundedVarcharType())
            .column(createColumnName("data_type"), BIGINT)
            .column(createColumnName("attr_type_name"), createUnboundedVarcharType())
            .column(createColumnName("attr_size"), BIGINT)
            .column(createColumnName("decimal_digits"), BIGINT)
            .column(createColumnName("num_prec_radix"), BIGINT)
            .column(createColumnName("nullable"), BIGINT)
            .column(createColumnName("remarks"), createUnboundedVarcharType())
            .column(createColumnName("attr_def"), createUnboundedVarcharType())
            .column(createColumnName("sql_data_type"), BIGINT)
            .column(createColumnName("sql_datetime_sub"), BIGINT)
            .column(createColumnName("char_octet_length"), BIGINT)
            .column(createColumnName("ordinal_position"), BIGINT)
            .column(createColumnName("is_nullable"), createUnboundedVarcharType())
            .column(createColumnName("scope_catalog"), createUnboundedVarcharType())
            .column(createColumnName("scope_schema"), createUnboundedVarcharType())
            .column(createColumnName("scope_table"), createUnboundedVarcharType())
            .column(createColumnName("source_data_type"), BIGINT)
            .build();

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return InMemoryRecordSet.builder(METADATA).build().cursor();
    }
}
