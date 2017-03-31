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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.guava.Sequence;

import java.util.Map;

/**
 */
public class TabularPostProcessor implements PostProcessingOperator.TabularOutput
{
  private final String timestampColumn;
  private final QueryToolChestWarehouse warehouse;

  @JsonCreator
  public TabularPostProcessor(
      @JsonProperty("timestampColumn") String timestampColumn,
      @JacksonInject QueryToolChestWarehouse warehouse
  )
  {
    this.timestampColumn = timestampColumn;
    this.warehouse = warehouse;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseQueryRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        QueryToolChest toolChest = warehouse.getToolChest(query);
        Sequence sequence = baseQueryRunner.run(query, responseContext);
        return toolChest == null ? sequence : toolChest.toTabularFormat(sequence, timestampColumn).getSequence();
      }
    };
  }
}