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

package io.druid.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.multibindings.MapBinder;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.datasourcemetadata.DataSourceMetadataQueryRunnerFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.kmeans.FindNearestQuery;
import io.druid.query.kmeans.FindNearestQueryRunnerFactory;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaQueryRunnerFactory;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryRunnerFactory;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.select.StreamRawQueryRunnerFactory;
import io.druid.query.sketch.SketchQuery;
import io.druid.query.sketch.SketchQueryRunnerFactory;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.server.QueryManager;

import java.util.Map;

/**
 */
public class QueryRunnerFactoryModule extends QueryToolChestModule
{
  private static final Map<Class<? extends Query>, Class<? extends QueryRunnerFactory>> mappings =
      ImmutableMap.<Class<? extends Query>, Class<? extends QueryRunnerFactory>>builder()
                  .put(TimeseriesQuery.class, TimeseriesQueryRunnerFactory.class)
                  .put(SearchQuery.class, SearchQueryRunnerFactory.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryRunnerFactory.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryRunnerFactory.class)
                  .put(GroupByQuery.class, GroupByQueryRunnerFactory.class)
                  .put(SelectMetaQuery.class, SelectMetaQueryRunnerFactory.class)
                  .put(SelectQuery.class, SelectQueryRunnerFactory.class)
                  .put(StreamQuery.class, StreamQueryRunnerFactory.class)
                  .put(StreamRawQuery.class, StreamRawQueryRunnerFactory.class)
                  .put(TopNQuery.class, TopNQueryRunnerFactory.class)
                  .put(DataSourceMetadataQuery.class, DataSourceMetadataQueryRunnerFactory.class)
                  .put(FindNearestQuery.class, FindNearestQueryRunnerFactory.class)
                  .put(SketchQuery.class, SketchQueryRunnerFactory.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    binder.bind(QueryWatcher.class)
          .to(QueryManager.class)
          .in(LazySingleton.class);
    binder.bind(QueryManager.class)
          .in(LazySingleton.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder = DruidBinders.queryRunnerFactoryBinder(
        binder
    );

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryRunnerFactory>> entry : mappings.entrySet()) {
      queryFactoryBinder.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    binder.bind(GroupByQueryEngine.class).in(LazySingleton.class);
    binder.bind(TopNQueryEngine.class).in(LazySingleton.class);
  }
}
