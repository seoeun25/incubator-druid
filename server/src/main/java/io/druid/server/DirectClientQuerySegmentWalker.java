/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.client.DirectDruidClient;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

/**
 */
@Deprecated
public class DirectClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryToolChestWarehouse warehouse;
  private final DirectDruidClient baseClient;

  public DirectClientQuerySegmentWalker(
      QueryToolChestWarehouse warehouse,
      DirectDruidClient baseClient
  )
  {
    this.warehouse = warehouse;
    this.baseClient = baseClient;
  }

  @Override
  public ObjectMapper getObjectMapper()
  {
    return baseClient.getObjectMapper();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query);
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> makeRunner(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    return FinalizeResultsQueryRunner.finalize(baseClient, toolChest, baseClient.getObjectMapper());
  }
}
