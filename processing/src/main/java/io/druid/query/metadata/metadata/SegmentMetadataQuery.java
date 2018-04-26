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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.utils.JodaUtils;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SegmentMetadataQuery extends BaseQuery<SegmentAnalysis> implements Query.VCSupport<SegmentAnalysis>
{
  /* The SegmentMetadataQuery cache key may contain UTF-8 column name strings.
   * Prepend 0xFF before the analysisTypes as a separator to avoid
   * any potential confusion with string values.
   */
  public static final byte[] ANALYSIS_TYPES_CACHE_PREFIX = new byte[]{(byte) 0xFF};

  public enum AnalysisType
  {
    CARDINALITY,
    SERIALIZED_SIZE,
    INTERVAL,
    AGGREGATORS,
    MINMAX,
    NULL_COUNT,
    QUERYGRANULARITY,
    INGESTED_NUMROW,
    ROLLUP,
    LAST_ACCESS_TIME;

    @JsonValue
    @Override
    public String toString()
    {
      return this.name().toLowerCase();
    }

    @JsonCreator
    public static AnalysisType fromString(String name)
    {
      return valueOf(name.toUpperCase());
    }

    public byte[] getCacheKey()
    {
      return new byte[]{(byte) this.ordinal()};
    }
  }

  public static final Interval DEFAULT_INTERVAL = new Interval(
      JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT
  );

  public static final EnumSet<AnalysisType> DEFAULT_NON_COLUMN_STATS = EnumSet.of(
      AnalysisType.SERIALIZED_SIZE,
      AnalysisType.LAST_ACCESS_TIME,
      AnalysisType.INGESTED_NUMROW,
      AnalysisType.INTERVAL
  );

  public static final EnumSet<AnalysisType> DEFAULT_ANALYSIS_TYPES = EnumSet.of(
      AnalysisType.CARDINALITY,
      AnalysisType.SERIALIZED_SIZE,
      AnalysisType.INTERVAL,
      AnalysisType.MINMAX
  );

  private final List<VirtualColumn> virtualColumns;
  private final ColumnIncluderator toInclude;
  private final boolean merge;
  private final boolean usingDefaultInterval;
  private final EnumSet<AnalysisType> analysisTypes;
  private final boolean lenientAggregatorMerge;

  @JsonCreator
  public SegmentMetadataQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("toInclude") ColumnIncluderator toInclude,
      @JsonProperty("merge") Boolean merge,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("analysisTypes") EnumSet<AnalysisType> analysisTypes,
      @JsonProperty("usingDefaultInterval") Boolean useDefaultInterval,
      @JsonProperty("lenientAggregatorMerge") Boolean lenientAggregatorMerge
  )
  {
    super(
        dataSource,
        (querySegmentSpec == null) ? new MultipleIntervalSegmentSpec(Arrays.asList(DEFAULT_INTERVAL))
                                   : querySegmentSpec,
        false,
        context
    );

    if (querySegmentSpec == null) {
      this.usingDefaultInterval = true;
    } else {
      this.usingDefaultInterval = useDefaultInterval == null ? false : useDefaultInterval;
    }
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.toInclude = toInclude == null ? new AllColumnIncluderator() : toInclude;
    this.merge = merge == null ? false : merge;
    this.analysisTypes = (analysisTypes == null) ? DEFAULT_ANALYSIS_TYPES : analysisTypes;
    Preconditions.checkArgument(
        dataSource instanceof TableDataSource,
        "SegmentMetadataQuery only supports table datasource"
    );
    this.lenientAggregatorMerge = lenientAggregatorMerge == null ? false : lenientAggregatorMerge;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public ColumnIncluderator getToInclude()
  {
    return toInclude;
  }

  @JsonProperty
  public boolean isMerge()
  {
    return merge;
  }

  @JsonProperty
  public boolean isUsingDefaultInterval()
  {
    return usingDefaultInterval;
  }

  @Override
  public String getType()
  {
    return Query.SEGMENT_METADATA;
  }

  @JsonProperty
  public EnumSet<AnalysisType> getAnalysisTypes()
  {
    return analysisTypes;
  }

  @JsonProperty
  public boolean isLenientAggregatorMerge()
  {
    return lenientAggregatorMerge;
  }

  public boolean analyzingOnlyInterval()
  {
    return analysisTypes.size() == 1 && analysisTypes.contains(AnalysisType.INTERVAL);
  }

  public boolean analyzingInterval()
  {
    return analysisTypes.contains(AnalysisType.INTERVAL);
  }
  public boolean hasAggregators()
  {
    return analysisTypes.contains(AnalysisType.AGGREGATORS);
  }

  public boolean hasQueryGranularity()
  {
    return analysisTypes.contains(AnalysisType.QUERYGRANULARITY);
  }

  public boolean hasRollup()
  {
    return analysisTypes.contains(AnalysisType.ROLLUP);
  }

  public boolean hasMinMax()
  {
    return analysisTypes.contains(AnalysisType.MINMAX);
  }

  public boolean hasIngestedNumRows()
  {
    return analysisTypes.contains(AnalysisType.INGESTED_NUMROW);
  }

  public boolean hasSerializedSize()
  {
    return analysisTypes.contains(AnalysisType.SERIALIZED_SIZE);
  }

  public boolean hasLastAccessTime()
  {
    return analysisTypes.contains(AnalysisType.LAST_ACCESS_TIME);
  }

  public byte[] getAnalysisTypesCacheKey()
  {
    int size = 1;
    List<byte[]> typeBytesList = Lists.newArrayListWithExpectedSize(analysisTypes.size());
    for (AnalysisType analysisType : analysisTypes) {
      final byte[] bytes = analysisType.getCacheKey();
      typeBytesList.add(bytes);
      size += bytes.length;
    }

    final ByteBuffer bytes = ByteBuffer.allocate(size);
    bytes.put(ANALYSIS_TYPES_CACHE_PREFIX);
    for (byte[] typeBytes : typeBytesList) {
      bytes.put(typeBytes);
    }

    return bytes.array();
  }


  @Override
  public SegmentMetadataQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        merge,
        computeOverriddenContext(contextOverride),
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge
    );
  }

  @Override
  public SegmentMetadataQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        spec,
        virtualColumns,
        toInclude,
        merge,
        getContext(),
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge
    );
  }

  @Override
  public SegmentMetadataQuery withDataSource(DataSource dataSource)
  {
    return new SegmentMetadataQuery(
        dataSource,
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        merge,
        getContext(),
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge
    );
  }

  public SegmentMetadataQuery withColumns(ColumnIncluderator includerator)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        includerator,
        merge,
        getContext(),
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge
    );
  }

  @Override
  public SegmentMetadataQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        merge,
        getContext(),
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge
    );
  }

  public SegmentMetadataQuery withMoreAnalysis(AnalysisType... moreAnalysis)
  {
    Set<AnalysisType> current = Sets.newHashSet(analysisTypes);
    current.addAll(Arrays.asList(moreAnalysis));
    EnumSet<AnalysisType> added = EnumSet.copyOf(current);
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        merge,
        getContext(),
        added,
        usingDefaultInterval,
        lenientAggregatorMerge
    );
  }

  @Override
  public String toString()
  {
    return "SegmentMetadataQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", virtualColumns=" + virtualColumns +
           ", toInclude=" + toInclude +
           ", merge=" + merge +
           ", usingDefaultInterval=" + usingDefaultInterval +
           ", analysisTypes=" + analysisTypes +
           ", lenientAggregatorMerge=" + lenientAggregatorMerge +
           '}';
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
    if (!super.equals(o)) {
      return false;
    }
    SegmentMetadataQuery that = (SegmentMetadataQuery) o;
    return merge == that.merge &&
           usingDefaultInterval == that.usingDefaultInterval &&
           lenientAggregatorMerge == that.lenientAggregatorMerge &&
           Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(toInclude, that.toInclude) &&
           Objects.equals(analysisTypes, that.analysisTypes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        virtualColumns,
        toInclude,
        merge,
        usingDefaultInterval,
        analysisTypes,
        lenientAggregatorMerge
    );
  }
}
