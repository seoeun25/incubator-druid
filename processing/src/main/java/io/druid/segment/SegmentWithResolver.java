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

package io.druid.segment;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.druid.query.RowResolver;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class SegmentWithResolver implements Segment.WithResolver
{
  private final Segment segment;
  private final Supplier<RowResolver> resolver;

  public SegmentWithResolver(Segment segment, Supplier<RowResolver> resolver) {
    this.segment = Preconditions.checkNotNull(segment);
    this.resolver = resolver;
  }

  @Override
  public String getIdentifier()
  {
    return segment.getIdentifier();
  }

  @Override
  public Interval getDataInterval()
  {
    return segment.getDataInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex(boolean forQuery)
  {
    return segment.asQueryableIndex(forQuery);
  }

  @Override
  public StorageAdapter asStorageAdapter(boolean forQuery)
  {
    return segment.asStorageAdapter(forQuery);
  }

  @Override
  public long getLastAccessTime()
  {
    return segment.getLastAccessTime();
  }

  @Override
  public void close() throws IOException
  {
    segment.close();
  }

  @Override
  public RowResolver resolver()
  {
    return resolver.get();
  }

  public Segment getSegment()
  {
    return segment;
  }
}