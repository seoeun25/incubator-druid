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

package io.druid.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.topn.TopNQueryConfig;

import javax.validation.constraints.NotNull;

/**
 */
public class QueryConfig
{
  @JsonProperty
  public int maxResults;

  @JsonProperty
  public boolean useCustomSerdeForDateTime;

  @JacksonInject
  @NotNull
  public Supplier<GroupByQueryConfig> groupBy = Suppliers.ofInstance(new GroupByQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<SearchQueryConfig> search = Suppliers.ofInstance(new SearchQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<SelectQueryConfig> select = Suppliers.ofInstance(new SelectQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<TopNQueryConfig> topN = Suppliers.ofInstance(new TopNQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<SegmentMetadataQueryConfig> segmentMeta = Suppliers.ofInstance(new SegmentMetadataQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<JoinQueryConfig> join = Suppliers.ofInstance(new JoinQueryConfig());

  public int getMaxResults()
  {
    return maxResults <= 0 ? getGroupBy().getMaxResults() : maxResults;
  }

  public boolean useCustomSerdeForDateTime(Query query)
  {
    if (!useCustomSerdeForDateTime || query.getContextBoolean(Query.DATETIME_STRING_SERDE, false) ) {
      return false;
    }
    if (query instanceof SelectQuery) {
      // events containing DateTime in Map (which means no type information)
      return !getSelect().isUseDateTime();
    }
    return true;
  }

  public GroupByQueryConfig getGroupBy()
  {
    return groupBy.get();
  }

  public SearchQueryConfig getSearch()
  {
    return search.get();
  }

  public SelectQueryConfig getSelect()
  {
    return select.get();
  }

  public TopNQueryConfig getTopN()
  {
    return topN.get();
  }

  public SegmentMetadataQueryConfig getSegmentMeta()
  {
    return segmentMeta.get();
  }

  public JoinQueryConfig getJoin()
  {
    return join.get();
  }
}
