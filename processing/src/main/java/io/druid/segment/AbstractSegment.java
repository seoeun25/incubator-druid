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

import io.druid.segment.data.Indexed;

public abstract class AbstractSegment implements Segment
{
  protected volatile long lastAccessTime;

  protected void accessed(boolean forQuery)
  {
    if (forQuery) {
      lastAccessTime = System.currentTimeMillis();
    }
  }

  @Override
  public Indexed<String> getAvailableDimensions(boolean forQuery)
  {
    QueryableIndex queryableIndex = asQueryableIndex(forQuery);
    if (queryableIndex != null) {
      return queryableIndex.getAvailableDimensions();
    }
    return asStorageAdapter(forQuery).getAvailableDimensions();
  }

  @Override
  public long getLastAccessTime()
  {
    return lastAccessTime;
  }
}
