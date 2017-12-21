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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import io.druid.data.TypeResolver;
import io.druid.data.input.Row;

/**
 */
public class LessThanOrEQHavingSpec extends CompareHavingSpec
{
  public LessThanOrEQHavingSpec(
      @JsonProperty("aggregation") String aggName,
      @JsonProperty("value") Number value
  )
  {
    super(aggName, value);
  }

  @Override
  public Predicate<Row> toEvaluator(TypeResolver resolver)
  {
    return new Predicate<Row>()
    {
      @Override
      public boolean apply(Row input)
      {
        return HavingSpecMetricComparator.compare(input, aggregationName, value) <= 0;
      }
    };
  }
}