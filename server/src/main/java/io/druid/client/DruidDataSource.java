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

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 */
public class DruidDataSource
{
  public static final Function<String, DruidDataSource> FACTORY = new Function<String, DruidDataSource>()
  {
    @Override
    public DruidDataSource apply(String datasourceName)
    {
      return new DruidDataSource(datasourceName, ImmutableMap.of("created", new DateTime().toString()));
    }
  };

  private final String name;
  private final Map<String, String> properties;
  private final Map<String, DataSegment> segmentsMap;

  public DruidDataSource(String name, Map<String, String> properties)
  {
    this.name = name;
    this.properties = properties;
    this.segmentsMap = Maps.newHashMap();
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @JsonProperty
  public synchronized List<DataSegment> getSegments()
  {
    return ImmutableList.copyOf(segmentsMap.values());
  }

  public List<DataSegment> getSegmentsSorted()
  {
    List<DataSegment> segments = Lists.newArrayList(getSegments());
    Collections.sort(segments);
    return segments;
  }

  public synchronized DruidDataSource addSegment(DataSegment dataSegment)
  {
    segmentsMap.put(dataSegment.getIdentifier(), dataSegment);
    return this;
  }

  public synchronized boolean addSegmentIfAbsent(DataSegment dataSegment)
  {
    return segmentsMap.putIfAbsent(dataSegment.getIdentifier(), dataSegment) == null;
  }

  public synchronized DruidDataSource addSegments(Map<String, DataSegment> partitionMap)
  {
    segmentsMap.putAll(partitionMap);
    return this;
  }

  public synchronized DruidDataSource removeSegment(String segmentId)
  {
    segmentsMap.remove(segmentId);
    return this;
  }

  public synchronized boolean contains(String id)
  {
    return segmentsMap.containsKey(id);
  }

  public synchronized boolean isEmpty()
  {
    return segmentsMap.isEmpty();
  }

  @Override
  public String toString()
  {
    return "DruidDataSource{" +
           "name=" + name +
           ", properties=" + properties +
           ", segmentsMap=" + segmentsMap +
           '}';
  }

  public synchronized ImmutableDruidDataSource toImmutableDruidDataSource()
  {
    return new ImmutableDruidDataSource(
        name,
        ImmutableMap.copyOf(properties),
        ImmutableMap.copyOf(segmentsMap)
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return name.equals(((DruidDataSource) o).name);
  }

  @Override
  public int hashCode()
  {
    return name.hashCode();
  }
}
