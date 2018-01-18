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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.GenericAggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

/**
 */
@JsonTypeName("variance")
public class VarianceAggregatorFactory extends GenericAggregatorFactory
{
  protected static final byte CACHE_TYPE_ID = 16;

  protected final String estimator;
  protected final boolean isVariancePop;

  @JsonCreator
  public VarianceAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("estimator") String estimator,
      @JsonProperty("inputType") String inputType
  )
  {
    super(name, fieldName, fieldExpression, predicate, inputType);
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
  }

  public VarianceAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null, null, null);
  }

  @Override
  public String getTypeName()
  {
    return "variance";
  }

  @Override
  public String getInputTypeName()
  {
    return inputType.typeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return VarianceAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return VarianceAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return VarianceAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return VarianceAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        switch (valueType.typeName()) {
          case "variance":
          case "varianceCombined":
            return VarianceAggregator.create(
                ColumnSelectors.getObjectColumnSelector(
                    metricFactory,
                    fieldName,
                    fieldExpression
                ),
                ColumnSelectors.toMatcher(predicate, metricFactory)
            );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, double, long or variance, got a %s", fieldName, inputType
    );
  }

  @Override
  protected BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return VarianceBufferAggregator.create(
            name,
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return VarianceBufferAggregator.create(
            name,
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return VarianceBufferAggregator.create(
            name,
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
      switch (valueType.typeName()) {
        case "variance":
        case "varianceCombined":
          return VarianceBufferAggregator.create(
              name,
              ColumnSelectors.getObjectColumnSelector(
                  metricFactory,
                  fieldName,
                  fieldExpression
              ),
              ColumnSelectors.toMatcher(predicate, metricFactory)
          );
      }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, double, long or variance, got a %s", fieldName, inputType
    );
  }

  @Override
  protected AggregatorFactory withValue(String name, String fieldName, String inputType)
  {
    return new VarianceAggregatorFactory(name, fieldName, null, null, estimator, inputType);
  }

  @Override
  protected byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public Comparator getComparator()
  {
    return VarianceAggregatorCollector.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return VarianceAggregatorCollector.combineValues(lhs, rhs);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((VarianceAggregatorCollector) object).getVariance(isVariancePop);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return VarianceAggregatorCollector.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return VarianceAggregatorCollector.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return VarianceAggregatorCollector.from(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
  }

  @JsonProperty
  public String getEstimator()
  {
    return estimator;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] superKey = super.getCacheKey();
    return ByteBuffer.allocate(superKey.length + 1)
                     .put(superKey)
                     .put(isVariancePop ? (byte) 1 : 0)
                     .array();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + '{' +
           "name='" + name + '\'' +
           (fieldName == null ? "": ", fieldName='" + fieldName + '\'') +
           (fieldExpression == null ? "": ", fieldExpression='" + fieldExpression + '\'') +
           (predicate == null ? "": ", predicate='" + predicate + '\'') +
           ", estimator='" + estimator + '\'' +
           ", inputType='" + inputType + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    VarianceAggregatorFactory that = (VarianceAggregatorFactory) o;
    return Objects.equals(isVariancePop, that.isVariancePop);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Objects.hashCode(isVariancePop);
    return result;
  }
}
