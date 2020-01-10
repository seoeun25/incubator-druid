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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class RequestLogLine
{
  private static final Joiner JOINER = Joiner.on("\t");

  private final DateTime timestamp;
  private final String remoteAddr;
  private final Object query;
  private final QueryStats queryStats;
  private final Map<String, Object> sqlQueryContext;

  public RequestLogLine(DateTime timestamp, String remoteAddr, Object query, QueryStats queryStats,
                        Map<String, Object> sqlQueryContext)
  {
    this.timestamp = timestamp;
    this.remoteAddr = remoteAddr;
    this.query = query;
    this.queryStats = queryStats;
    this.sqlQueryContext = sqlQueryContext;
  }

  public String getLine(ObjectMapper objectMapper) throws JsonProcessingException
  {
    return JOINER.join(
        Arrays.asList(
            timestamp,
            remoteAddr,
            objectMapper.writeValueAsString(query),
            objectMapper.writeValueAsString(queryStats),
            objectMapper.writeValueAsString(Optional.ofNullable(sqlQueryContext).orElse(ImmutableMap.of()))
        )
    );
  }

  @JsonProperty("timestamp")
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty("query")
  public Object getQuery()
  {
    return query;
  }

  @JsonProperty("remoteAddr")
  public String getRemoteAddr()
  {
    return remoteAddr;
  }

  @JsonProperty("queryStats")
  public QueryStats getQueryStats()
  {
    return queryStats;
  }

  @Nullable
  @JsonProperty
  public Map<String, Object> getSqlQueryContext()
  {
    return sqlQueryContext;
  }

  @Override
  public String toString()
  {
    return "RequestLogLine{" +
           "query=" + query +
           ", sqlQueryContext=" + sqlQueryContext +
           ", timestamp=" + timestamp +
           ", remoteAddr='" + remoteAddr + '\'' +
           ", queryStats=" + queryStats +
           '}';
  }
}
