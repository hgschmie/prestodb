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
package com.facebook.presto.tests;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;

public abstract class AbstractTestingPrestoClient<T>
        implements Closeable
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private final TestingPrestoServer prestoServer;
    private final ConnectorSession defaultSession;

    private final HttpClient httpClient;

    protected AbstractTestingPrestoClient(TestingPrestoServer prestoServer,
            ConnectorSession defaultSession)
    {
        this.prestoServer = checkNotNull(prestoServer, "prestoServer is null");
        this.defaultSession = checkNotNull(defaultSession, "defaultSession is null");

        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setReadTimeout(new Duration(10, TimeUnit.DAYS)));
    }

    @Override
    public void close()
    {
        this.httpClient.close();
    }

    protected abstract ResultsSession<T> getResultSession(ConnectorSession session);

    public T execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    public T execute(ConnectorSession session, @Language("SQL") String sql)
    {
        ResultsSession<T> resultsSession = getResultSession(session);

        try (StatementClient client = new StatementClient(httpClient, QUERY_RESULTS_CODEC, toClientSession(session), sql)) {
            while (client.isValid()) {
                QueryResults results = client.current();

                resultsSession.addResults(results);
                client.advance();
            }

            if (!client.isFailed()) {
                return resultsSession.build();
            }

            QueryError error = client.finalResults().getError();
            assert error != null;
            if (error.getFailureInfo() != null) {
                throw error.getFailureInfo().toException();
            }
            throw new RuntimeException("Query failed: " + error.getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    public List<QualifiedTableName> listTables(ConnectorSession session, String catalog, String schema)
    {
        return prestoServer.getMetadata().listTables(session, new QualifiedTablePrefix(catalog, schema));
    }

    public boolean tableExists(ConnectorSession session, String table)
    {
        QualifiedTableName name = new QualifiedTableName(session.getCatalog(), session.getSchema(), table);
        Optional<TableHandle> handle = prestoServer.getMetadata().getTableHandle(session, name);
        return handle.isPresent();
    }

    public ConnectorSession getDefaultSession()
    {
        return defaultSession;
    }

    public TestingPrestoServer getServer()
    {
        return prestoServer;
    }

    public ClientSession toClientSession(ConnectorSession connectorSession)
    {
        return new ClientSession(
                prestoServer.getBaseUrl(),
                connectorSession.getUser(),
                connectorSession.getSource(),
                connectorSession.getCatalog(),
                connectorSession.getSchema(),
                connectorSession.getTimeZoneKey().getId(),
                connectorSession.getLocale(), true);
    }

    protected List<Type> getTypes(List<Column> columns)
    {
        return ImmutableList.copyOf(transform(columns, columnTypeGetter()));
    }

    protected Function<Column, Type> columnTypeGetter()
    {
        return new Function<Column, Type>()
        {
            @Override
            public Type apply(Column column)
            {
                String typeName = column.getType();
                Type type = prestoServer.getMetadata().getType(typeName);
                if (type == null) {
                    throw new AssertionError("Unhandled type: " + typeName);
                }
                return type;
            }
        };
    }
}
