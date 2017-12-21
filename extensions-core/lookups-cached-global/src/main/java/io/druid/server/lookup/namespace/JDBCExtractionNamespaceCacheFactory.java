/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.lookup.namespace;

import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.JDBCExtractionNamespace;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.lang.StringUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ConnectionFactory;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.TimestampMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class JDBCExtractionNamespaceCacheFactory
    implements ExtractionNamespaceCacheFactory<JDBCExtractionNamespace>
{
  private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactory.class);
  private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();

  @Override
  public Callable<String> getCachePopulator(
      final String id,
      final JDBCExtractionNamespace namespace,
      final String lastVersion,
      final Map<Object, String> cache
  )
  {
    final long lastCheck = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    final Long lastDBUpdate = lastUpdates(id, namespace);
    if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
      return new Callable<String>()
      {
        @Override
        public String call() throws Exception
        {
          return lastVersion;
        }
      };
    }
    return new Callable<String>()
    {
      @Override
      public String call()
      {
        final DBI dbi = ensureDBI(id, namespace);
        final String table = namespace.getTable();
        final String valueColumn = namespace.getValueColumn();
        final List<String> keyColumns = namespace.getKeyColumns();

        LOG.debug("Updating [%s]", id);
        final List<Pair<Object, String>> pairs = dbi.withHandle(
            new HandleCallback<List<Pair<Object, String>>>()
            {
              @Override
              public List<Pair<Object, String>> withHandle(Handle handle) throws Exception
              {
                final String query;
                query = String.format(
                    "SELECT %s, %s FROM %s",
                    StringUtils.join(keyColumns, ", "),
                    valueColumn,
                    table
                );
                if (keyColumns.size() > 1) {
                  return handle
                      .createQuery(
                          query
                      ).map(
                          new ResultSetMapper<Pair<Object, String>>()
                          {

                            @Override
                            public Pair<Object, String> map(
                                final int index,
                                final ResultSet r,
                                final StatementContext ctx
                            ) throws SQLException
                            {
                              String[] key = new String[keyColumns.size()];
                              int idx = 0;
                              for (String keyColumn: keyColumns) {
                                key[idx++] = r.getString(keyColumn);
                              }
                              return new Pair<Object, String>(new MultiKey(key, false), r.getString(valueColumn));
                            }
                          }
                      ).list();
                } else {
                  return handle
                      .createQuery(
                          query
                      ).map(
                          new ResultSetMapper<Pair<Object, String>>()
                          {

                            @Override
                            public Pair<Object, String> map(
                                final int index,
                                final ResultSet r,
                                final StatementContext ctx
                            ) throws SQLException
                            {

                              return new Pair<Object, String>(r.getString(keyColumns.get(0)), r.getString(valueColumn));
                            }
                          }
                      ).list();
                }
              }
            }
        );
        for (Pair<Object, String> pair : pairs) {
          cache.put(pair.lhs, pair.rhs);
        }
        LOG.info("Finished loading %d values for namespace[%s]", cache.size(), id);
        return String.format("%d", System.currentTimeMillis());
      }
    };
  }

  private DBI ensureDBI(String id, JDBCExtractionNamespace namespace)
  {
    final String key = id;
    DBI dbi = null;
    if (dbiCache.containsKey(key)) {
      dbi = dbiCache.get(key);
    }
    if (dbi == null) {
      final MetadataStorageConnectorConfig config = namespace.getConnectorConfig();
      final DBI newDbi = new DBI(
          new ConnectionFactory()
          {
            @Override
            public Connection openConnection() throws SQLException
            {
              return DriverManager.getConnection(config.getConnectURI(), config.getUser(), config.getPassword());
            }
          }
      );
      dbiCache.putIfAbsent(key, newDbi);
      dbi = dbiCache.get(key);
    }
    return dbi;
  }

  private Long lastUpdates(String id, JDBCExtractionNamespace namespace)
  {
    final DBI dbi = ensureDBI(id, namespace);
    final String table = namespace.getTable();
    final String tsColumn = namespace.getTsColumn();
    if (tsColumn == null) {
      return null;
    }
    final Timestamp update = dbi.withHandle(
        new HandleCallback<Timestamp>()
        {

          @Override
          public Timestamp withHandle(Handle handle) throws Exception
          {
            final String query = String.format(
                "SELECT MAX(%s) FROM %s",
                tsColumn, table
            );
            return handle
                .createQuery(query)
                .map(TimestampMapper.FIRST)
                .first();
          }
        }
    );
    return update.getTime();
  }

}