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

package io.druid.query.aggregation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.data.ValueType;
import io.druid.query.Cacheable;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Processing related interface
 * 
 * An AggregatorFactory is an object that knows how to generate an Aggregator using a ColumnSelectorFactory.
 * 
 * This is useful as an abstraction to allow Aggregator classes to be written in terms of MetricSelector objects
 * without making any assumptions about how they are pulling values out of the base data.  That is, the data is
 * provided to the Aggregator through the MetricSelector object, so whatever creates that object gets to choose how
 * the data is actually stored and accessed.
 */
public abstract class AggregatorFactory implements Cacheable
{
  private static final Logger log = new Logger(AggregatorFactory.class);

  public abstract Aggregator factorize(ColumnSelectorFactory metricFactory);

  public abstract BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory);

  public abstract Comparator getComparator();

  /**
   * A method that knows how to combine the outputs of the getIntermediate() method from the Aggregators
   * produced via factorize().  Note, even though this is called combine, this method's contract *does*
   * allow for mutation of the input objects.  Thus, any use of lhs or rhs after calling this method is
   * highly discouraged.
   *
   * @param lhs The left hand side of the combine
   * @param rhs The right hand side of the combine
   *
   * @return an object representing the combination of lhs and rhs, this can be a new object or a mutation of the inputs
   */
  public abstract Object combine(Object lhs, Object rhs);

  /**
   * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory.  This
   * generally amounts to simply creating a new factory that is the same as the current except with its input
   * column renamed to the same as the output column.
   *
   * @return a new Factory that can be used for operations on top of data output from the current factory.
   */
  public abstract AggregatorFactory getCombiningFactory();

  /**
   * Returns an AggregatorFactory that can be used to merge the output of aggregators from this factory and
   * other factory.
   * This method is relevant only for AggregatorFactory which can be used at ingestion time.
   *
   * @return a new Factory that can be used for merging the output of aggregators from this factory and other.
   */
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    throw new UnsupportedOperationException(String.format("[%s] does not implement getMergingFactory(..)", this.getClass().getName()));
  }

  @SuppressWarnings("unchecked")
  protected <T extends AggregatorFactory> T checkMergeable(AggregatorFactory other)
      throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return (T)other;
    }
    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  /**
   * A method that knows how to "deserialize" the object from whatever form it might have been put into
   * in order to transfer via JSON.
   *
   * @param object the object to deserialize
   *
   * @return the deserialized object
   */
  public abstract Object deserialize(Object object);

  /**
   * "Finalizes" the computation of an object.  Primarily useful for complex types that have a different mergeable
   * intermediate format than their final resultant output.
   *
   * @param object the object to be finalized
   *
   * @return the finalized value that should be returned for the initial query
   */
  public abstract Object finalizeComputation(Object object);

  public abstract String getName();

  public abstract List<String> requiredFields();

  public abstract byte[] getCacheKey();

  public abstract String getTypeName();

  // this is type for ingestion, which can be different from typeName (which is output type from serde)
  public String getInputTypeName()
  {
    return getTypeName();
  }

  /**
   * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
   *
   * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
   */
  public abstract int getMaxIntermediateSize();

  /**
   * Returns the starting value for a corresponding aggregator. For example, 0 for sums, - Infinity for max, an empty mogrifier
   *
   * @return the starting value for a corresponding aggregator.
   */
  public abstract Object getAggregatorStartValue();

  /**
   * This AggregatorFactory returns EstimableAggregator which provides more closer estimation of memory usage in ingestion
   *
   * @return
   */
  public boolean providesEstimation()
  {
    return false;
  }

  /**
   * Merges the list of AggregatorFactory[] (presumable from metadata of some segments being merged) and
   * returns merged AggregatorFactory[] (for the metadata for merged segment).
   * Null is returned if it is not possible to do the merging for any of the following reason.
   * - one of the element in input list is null i.e. aggregators for one the segments being merged is unknown
   * - AggregatorFactory of same name can not be merged if they are not compatible
   *
   * @param aggregatorsList
   *
   * @return merged AggregatorFactory[] or Null if merging is not possible.
   */
  public static AggregatorFactory[] mergeAggregators(List<AggregatorFactory[]> aggregatorsList)
  {
    if (aggregatorsList == null || aggregatorsList.isEmpty()) {
      return null;
    }

    Map<String, AggregatorFactory> mergedAggregators = new LinkedHashMap<>();

    for (AggregatorFactory[] aggregators : aggregatorsList) {

      if (aggregators != null) {
        for (AggregatorFactory aggregator : aggregators) {
          String name = aggregator.getName();
          if (mergedAggregators.containsKey(name)) {
            AggregatorFactory other = mergedAggregators.get(name);
            try {
              mergedAggregators.put(name, other.getMergingFactory(aggregator));
            }
            catch (AggregatorFactoryNotMergeableException ex) {
              log.warn(ex, "failed to merge aggregator factories");
              mergedAggregators = null;
              break;
            }
          } else {
            mergedAggregators.put(name, aggregator);
          }
        }
      } else {
        mergedAggregators = null;
        break;
      }
    }

    return mergedAggregators == null
           ? null
           : mergedAggregators.values().toArray(new AggregatorFactory[mergedAggregators.size()]);
  }

  public static Map<String, ValueType> toExpectedInputType(AggregatorFactory[] aggregators)
  {
    Map<String, ValueType> types = Maps.newHashMap();
    for (AggregatorFactory factory : aggregators) {
      Set<String> required = Sets.newHashSet(factory.requiredFields());
      if (required.size() == 1) {
        types.put(Iterables.getOnlyElement(required), ValueType.fromString(factory.getInputTypeName()));
      }
    }
    return types;
  }

  public static List<AggregatorFactory> toCombiner(List<AggregatorFactory> aggregators)
  {
    List<AggregatorFactory> combiners = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      combiners.add(aggregator.getCombiningFactory());
    }
    return combiners;
  }

  public static List<AggregatorFactory> toRelay(List<String> metrics, String type)
  {
    List<AggregatorFactory> relay = Lists.newArrayList();
    for (String metric : metrics) {
      relay.add(new RelayAggregatorFactory(metric, metric, type));
    }
    return relay;
  }

  public static List<AggregatorFactory> toRelay(List<AggregatorFactory> aggregators)
  {
    List<AggregatorFactory> relay = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      relay.add(new RelayAggregatorFactory(aggregator.getName(), aggregator.getName(), aggregator.getTypeName()));
    }
    return relay;
  }

  public static Function<AggregatorFactory, String> NAME = new Function<AggregatorFactory, String>()
  {
    @Override
    public String apply(AggregatorFactory input)
    {
      return input.getName();
    }
  };
}
