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

package io.druid.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.hash.Hashing;
import io.druid.data.ValueType;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.SelectEachQuery;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.ArrayAggregatorFactory;
import io.druid.query.aggregation.ArrayMetricSerde;
import io.druid.query.aggregation.CollectionCountPostAggregator;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DecimalMetricSerde;
import io.druid.query.aggregation.DimensionArrayAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.HistogramAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.ListAggregatorFactory;
import io.druid.query.aggregation.ListFoldingAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.aggregation.SetAggregatorFactory;
import io.druid.query.aggregation.TimestampMaxAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.aggregation.model.HoltWintersPostProcessor;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.JavaScriptPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StringMetricSerde;

/**
 */
public class AggregatorsModule extends SimpleModule
{
  public AggregatorsModule()
  {
    super("AggregatorFactories");

    if (ComplexMetrics.getSerdeForType("object") == null) {
      ComplexMetrics.registerSerde("object", new ComplexMetricSerde.Dummy());
    }
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }
    if (ComplexMetrics.getSerdeForType("array") == null) {
      ComplexMetrics.registerSerde("array", new ArrayMetricSerde(ValueType.FLOAT));
      ComplexMetrics.registerSerde("array.float", new ArrayMetricSerde(ValueType.FLOAT));
      ComplexMetrics.registerSerde("array.double", new ArrayMetricSerde(ValueType.DOUBLE));
      ComplexMetrics.registerSerde("array.long", new ArrayMetricSerde(ValueType.LONG));
      ComplexMetrics.registerSerde("array.string", new ArrayMetricSerde(ValueType.STRING));
    }
    if (ComplexMetrics.getSerdeForType("string") == null) {
      ComplexMetrics.registerSerde("string", StringMetricSerde.INSTANCE);
    }
    if (ComplexMetrics.getSerdeForType("decimal") == null) {
      ComplexMetrics.registerSerde("decimal", new DecimalMetricSerde());
    }
    if (ComplexMetrics.getSerdeFactory("decimal") == null) {
      ComplexMetrics.registerSerdeFactory("decimal", new DecimalMetricSerde.Factory());
    }

    setMixInAnnotation(AggregatorFactory.class, AggregatorFactoryMixin.class);
    setMixInAnnotation(PostAggregator.class, PostAggregatorMixin.class);
    setMixInAnnotation(Row.class, Rows.class);

    // for test
    registerSubtypes(SelectEachQuery.class);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "count", value = CountAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longSum", value = LongSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleSum", value = DoubleSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMax", value = DoubleMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMin", value = DoubleMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptAggregatorFactory.class),
      @JsonSubTypes.Type(name = "histogram", value = HistogramAggregatorFactory.class),
      @JsonSubTypes.Type(name = "hyperUnique", value = HyperUniquesAggregatorFactory.class),
      @JsonSubTypes.Type(name = "cardinality", value = CardinalityAggregatorFactory.class),
      @JsonSubTypes.Type(name = "filtered", value = FilteredAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longMax", value = LongMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longMin", value = LongMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "timeMax", value = TimestampMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "sum", value = GenericSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "min", value = GenericMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "max", value = GenericMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "list", value = ListAggregatorFactory.class),
      @JsonSubTypes.Type(name = "listFold", value = ListFoldingAggregatorFactory.class),
      @JsonSubTypes.Type(name = "set", value = SetAggregatorFactory.class),
      @JsonSubTypes.Type(name = "dimArray", value = DimensionArrayAggregatorFactory.class),
      @JsonSubTypes.Type(name = "array", value = ArrayAggregatorFactory.class),
      @JsonSubTypes.Type(name = "relay", value = RelayAggregatorFactory.class)
  })
  public static interface AggregatorFactoryMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "math", value = MathPostAggregator.class),
      @JsonSubTypes.Type(name = "arithmetic", value = ArithmeticPostAggregator.class),
      @JsonSubTypes.Type(name = "fieldAccess", value = FieldAccessPostAggregator.class),
      @JsonSubTypes.Type(name = "constant", value = ConstantPostAggregator.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptPostAggregator.class),
      @JsonSubTypes.Type(name = "hyperUniqueCardinality", value = HyperUniqueFinalizingPostAggregator.class),
      @JsonSubTypes.Type(name = "count", value = CollectionCountPostAggregator.class)
  })
  public static interface PostAggregatorMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "version", defaultImpl = MapBasedRow.class)
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "v1", value = MapBasedRow.class),
      @JsonSubTypes.Type(name = "predict", value = HoltWintersPostProcessor.PredictedRow.class)
  })
  public static interface Rows
  {
  }
}
