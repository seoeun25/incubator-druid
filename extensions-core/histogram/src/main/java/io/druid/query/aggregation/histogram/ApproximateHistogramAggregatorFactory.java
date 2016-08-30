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

package io.druid.query.aggregation.histogram;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("approxHistogram")
public class ApproximateHistogramAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 12;

  protected final String name;
  protected final String fieldName;

  protected final int resolution;
  protected final int numBuckets;

  protected final float lowerLimit;
  protected final float upperLimit;

  protected final boolean compact;
  protected final boolean base64;   // use this only when input is base64 encoded ApproximateHistogramHolder instance
  protected final String predicate;

  @JsonCreator
  public ApproximateHistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("resolution") Integer resolution,
      @JsonProperty("numBuckets") Integer numBuckets,
      @JsonProperty("lowerLimit") Float lowerLimit,
      @JsonProperty("upperLimit") Float upperLimit,
      @JsonProperty("compact") Boolean compact,
      @JsonProperty("base64") Boolean base64,
      @JsonProperty("predicate") String predicate

  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.resolution = resolution == null ? ApproximateHistogram.DEFAULT_HISTOGRAM_SIZE : resolution;
    this.numBuckets = numBuckets == null ? ApproximateHistogram.DEFAULT_BUCKET_SIZE : numBuckets;
    this.lowerLimit = lowerLimit == null ? Float.NEGATIVE_INFINITY : lowerLimit;
    this.upperLimit = upperLimit == null ? Float.POSITIVE_INFINITY : upperLimit;
    this.compact = compact == null ? false: compact;
    this.base64 = base64 == null ? false: base64;
    this.predicate = predicate;

    Preconditions.checkArgument(this.resolution > 0, "resolution must be greater than 1");
    Preconditions.checkArgument(this.numBuckets > 0, "numBuckets must be greater than 1");
    Preconditions.checkArgument(this.upperLimit > this.lowerLimit, "upperLimit must be greater than lowerLimit");
  }

  public ApproximateHistogramAggregatorFactory(
      String name,
      String fieldName,
      int resolution,
      int numBuckets,
      float lowerLimit,
      float upperLimit,
      boolean compact
  )
  {
    this(name, fieldName, resolution, numBuckets, lowerLimit, upperLimit, compact, null, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new ApproximateHistogramAggregator(
        name,
        metricFactory.makeFloatColumnSelector(fieldName),
        resolution,
        lowerLimit,
        upperLimit,
        compact,
        ColumnSelectors.toPredicate(predicate, metricFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new ApproximateHistogramBufferAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        resolution,
        lowerLimit,
        upperLimit,
        ColumnSelectors.toPredicate(predicate, metricFactory)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return ApproximateHistogramAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return ApproximateHistogramAggregator.combineHistograms(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ApproximateHistogramFoldingAggregatorFactory(
        name,
        name,
        resolution,
        numBuckets,
        lowerLimit,
        upperLimit,
        compact,
        base64,
        null
    );
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof ApproximateHistogramAggregatorFactory) {
      ApproximateHistogramAggregatorFactory castedOther = (ApproximateHistogramAggregatorFactory) other;

      return new ApproximateHistogramFoldingAggregatorFactory(
          name,
          name,
          Math.max(resolution, castedOther.resolution),
          numBuckets,
          Math.min(lowerLimit, castedOther.lowerLimit),
          Math.max(upperLimit, castedOther.upperLimit),
          compact,
          base64,
          null
      );

    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(
        new ApproximateHistogramAggregatorFactory(
            fieldName,
            fieldName,
            resolution,
            numBuckets,
            lowerLimit,
            upperLimit,
            compact,
            base64,
            predicate
        )
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof ApproximateHistogramHolder) {
      return object;
    }
    final ApproximateHistogramHolder ah = compact ? new ApproximateCompactHistogram() : new ApproximateHistogram();
    if (object instanceof byte[]) {
      ah.fromBytes((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      ah.fromBytes((ByteBuffer) object);
    } else if (object instanceof String) {
      ah.fromBytes(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      throw new IllegalArgumentException("Invalid object"  + object);
    }
    ah.setLowerLimit(lowerLimit);
    ah.setUpperLimit(upperLimit);

    return ah;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((ApproximateHistogramHolder) object).toHistogram(numBuckets);
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getResolution()
  {
    return resolution;
  }

  @JsonProperty
  public float getLowerLimit()
  {
    return lowerLimit;
  }

  @JsonProperty
  public float getUpperLimit()
  {
    return upperLimit;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public boolean isCompact()
  {
    return compact;
  }

  @JsonProperty
  public boolean isBase64()
  {
    return base64;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);
    return ByteBuffer.allocate(3 + fieldNameBytes.length + Ints.BYTES * 2 + Floats.BYTES * 2 + predicateBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(compact ? (byte) 1 : 0)
                     .put(base64 ? (byte) 1 : 0)
                     .put(fieldNameBytes)
                     .put(predicateBytes)
                     .putInt(resolution)
                     .putInt(numBuckets)
                     .putFloat(lowerLimit)
                     .putFloat(upperLimit).array();
  }

  @Override
  public String getTypeName()
  {
    return compact ?
                      base64 ? "approximateBase64CompactHistogram" : "approximateCompactHistogram"
                   :
                      base64 ? "approximateBase64Histogram" : "approximateHistogram";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return getEmptyHistogram().getMaxStorageSize();
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return getEmptyHistogram();
  }

  private ApproximateHistogramHolder getEmptyHistogram() {
    return compact ? new ApproximateCompactHistogram(resolution) : new ApproximateHistogram(resolution);
  }

  @Override
  public boolean providesEstimation()
  {
    return true;
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

    ApproximateHistogramAggregatorFactory that = (ApproximateHistogramAggregatorFactory) o;

    if (compact != that.compact) {
      return false;
    }
    if (base64 != that.base64) {
      return false;
    }
    if (Float.compare(that.lowerLimit, lowerLimit) != 0) {
      return false;
    }
    if (numBuckets != that.numBuckets) {
      return false;
    }
    if (resolution != that.resolution) {
      return false;
    }
    if (Float.compare(that.upperLimit, upperLimit) != 0) {
      return false;
    }
    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (compact ? 1 : 0);
    result = 31 * result + (base64 ? 1 : 0);
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + resolution;
    result = 31 * result + numBuckets;
    result = 31 * result + (lowerLimit != +0.0f ? Float.floatToIntBits(lowerLimit) : 0);
    result = 31 * result + (upperLimit != +0.0f ? Float.floatToIntBits(upperLimit) : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ApproximateHistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", compact=" + compact +
           ", resolution=" + resolution +
           ", numBuckets=" + numBuckets +
           ", lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           '}';
  }
}
