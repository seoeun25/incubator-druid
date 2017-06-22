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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.cache.Cache;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class StreamQueryEngine
{
  public Pair<Schema, Sequence<Object[]>> process(
      final AbstractStreamQuery baseQuery,
      final Segment segment,
      final Cache cache
  )
  {
    final StorageAdapter adapter = segment.asStorageAdapter(true);

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final AbstractStreamQuery<?> query = (AbstractStreamQuery) ViewSupportHelper.rewrite(baseQuery, adapter);

    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final String concatString = query.getConcatString();
    final Schema schema = ViewSupportHelper.toSchema(query, segment).appendTime();

    final List<DimensionSpec> dimensions = query.getDimensions();
    final List<String> metrics = query.getMetrics();

    Sequence<Object[]> sequence = Sequences.concat(
        QueryRunnerHelper.makeCursorBasedQuery(
            adapter,
            Iterables.getOnlyElement(intervals),
            VirtualColumns.valueOf(query.getVirtualColumns()),
            query.getDimensionsFilter(),
            cache,
            query.isDescending(),
            query.getGranularity(),
            new Function<Cursor, Sequence<Object[]>>()
            {
              @Override
              public Sequence<Object[]> apply(final Cursor cursor)
              {
                final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

                final DimensionSelector[] dimSelectors = new DimensionSelector[dimensions.size()];
                for (int i = 0; i < dimSelectors.length; i++) {
                  dimSelectors[i] = cursor.makeDimensionSelector(dimensions.get(i));
                }
                final ObjectColumnSelector[] metSelectors = new ObjectColumnSelector[metrics.size()];
                for (int i = 0; i < metSelectors.length; i++) {
                  metSelectors[i] = cursor.makeObjectColumnSelector(metrics.get(i));
                }

                return Sequences.simple(
                    new Iterable<Object[]>()
                    {
                      @Override
                      public Iterator<Object[]> iterator()
                      {
                        return new Iterator<Object[]>()
                        {
                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone();
                          }

                          @Override
                          public Object[] next()
                          {
                            final Object[] theEvent = new Object[schema.size()];

                            int index = 0;
                            for (DimensionSelector selector : dimSelectors) {
                              if (selector == null) {
                                continue;
                              }
                              final IndexedInts vals = selector.getRow();
                              final int size = vals.size();
                              if (size == 1) {
                                theEvent[index++] = selector.lookupName(vals.get(0));
                              } else {
                                String[] dimVals = new String[size];
                                for (int i = 0; i < size; ++i) {
                                  dimVals[i] = selector.lookupName(vals.get(i));
                                }
                                if (concatString != null) {
                                  theEvent[index++] = StringUtils.join(dimVals, concatString);
                                } else {
                                  theEvent[index++] = Arrays.asList(dimVals);
                                }
                              }
                            }

                            for (ObjectColumnSelector selector : metSelectors) {
                              if (selector != null) {
                                theEvent[index++] = selector.get();
                              }
                            }
                            theEvent[index] = timestampColumnSelector.get();

                            cursor.advance();
                            return theEvent;
                          }
                        };
                      }
                    }
                );
              }
            }
        )
    );

    return Pair.of(schema, sequence);
  }
}
