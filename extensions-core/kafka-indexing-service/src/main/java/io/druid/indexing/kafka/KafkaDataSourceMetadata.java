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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.java.util.common.IAE;
import io.druid.indexing.overlord.BaseDataSourceMetadata;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.metadata.TableDesc;

import java.util.Map;
import java.util.Objects;

public class KafkaDataSourceMetadata extends BaseDataSourceMetadata
{
  private final KafkaPartitions kafkaPartitions;

  @JsonCreator
  public KafkaDataSourceMetadata(
      @JsonProperty("partitions") KafkaPartitions kafkaPartitions,
      @JsonProperty("tableDesc") TableDesc tableDesc
  )
  {
    super(tableDesc);
    this.kafkaPartitions = kafkaPartitions;
  }

  public KafkaDataSourceMetadata(KafkaPartitions kafkaPartitions)
  {
    this(kafkaPartitions, null);
  }

  @JsonProperty("partitions")
  public KafkaPartitions getKafkaPartitions()
  {
    return kafkaPartitions;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (getClass() != other.getClass()) {
      return false;
    }

    return Objects.equals(
        ((KafkaDataSourceMetadata) this.plus(other)).kafkaPartitions,
        ((KafkaDataSourceMetadata) other.plus(this)).kafkaPartitions
    );
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    if (!(other instanceof KafkaDataSourceMetadata)) {
      throw new IAE(
          "Expected instance of %s, got %s",
          KafkaDataSourceMetadata.class.getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }
    final TableDesc merged = updateProperty(other.getTableDesc());

    final KafkaDataSourceMetadata that = (KafkaDataSourceMetadata) other;

    if (that.getKafkaPartitions().getTopic().equals(kafkaPartitions.getTopic())) {
      // Same topic, merge offsets.
      final Map<Integer, Long> newMap = Maps.newHashMap();

      for (Map.Entry<Integer, Long> entry : kafkaPartitions.getPartitionOffsetMap().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<Integer, Long> entry : that.getKafkaPartitions().getPartitionOffsetMap().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      return new KafkaDataSourceMetadata(new KafkaPartitions(kafkaPartitions.getTopic(), newMap), merged);
    } else {
      // Different topic, prefer "other".
      return new KafkaDataSourceMetadata(that.getKafkaPartitions(), merged);
    }
  }


  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    if (!(other instanceof KafkaDataSourceMetadata)) {
      throw new IAE(
          "Expected instance of %s, got %s",
          KafkaDataSourceMetadata.class.getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final KafkaDataSourceMetadata that = (KafkaDataSourceMetadata) other;

    if (that.getKafkaPartitions().getTopic().equals(kafkaPartitions.getTopic())) {
      // Same topic, remove partitions present in "that" from "this"
      final Map<Integer, Long> newMap = Maps.newHashMap();

      for (Map.Entry<Integer, Long> entry : kafkaPartitions.getPartitionOffsetMap().entrySet()) {
        if (!that.getKafkaPartitions().getPartitionOffsetMap().containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
        }
      }

      return new KafkaDataSourceMetadata(new KafkaPartitions(kafkaPartitions.getTopic(), newMap));
    } else {
      // Different topic, prefer "this".
      return this;
    }
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
    KafkaDataSourceMetadata that = (KafkaDataSourceMetadata) o;
    return super.equals(o) && Objects.equals(kafkaPartitions, that.kafkaPartitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), kafkaPartitions);
  }

  @Override
  public String toString()
  {
    return "KafkaDataSourceMetadata{" +
           "kafkaPartitions=" + kafkaPartitions +
           "tableDesc=" + getTableDesc() +
           '}';
  }
}
