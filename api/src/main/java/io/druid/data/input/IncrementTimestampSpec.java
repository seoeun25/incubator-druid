/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class IncrementTimestampSpec implements TimestampSpec
{
  private final AtomicLong counter;
  private final long start;
  private final int increment;

  @JsonCreator
  public IncrementTimestampSpec(
      @JsonProperty("start") long start,
      @JsonProperty("increment") int increment
  )
  {
    this.start = start;
    this.increment = increment == 0 ? 1 : increment;
    this.counter = new AtomicLong(start);
  }

  @JsonProperty
  public long getStart()
  {
    return start;
  }

  @JsonProperty
  public int getIncrement()
  {
    return increment;
  }

  @Override
  public String getTimestampColumn()
  {
    return null;
  }

  @Override
  public DateTime extractTimestamp(Map<String, Object> input)
  {
    return new DateTime(counter.getAndAdd(increment));
  }
}