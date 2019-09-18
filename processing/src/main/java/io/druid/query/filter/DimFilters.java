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

package io.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.Ranges;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Expressions;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class DimFilters
{
  private static final Logger LOG = new Logger(DimFilters.class);
  private static final Expression.Factory<DimFilter> FACTORY = new DimFilter.Factory();

  public static SelectorDimFilter dimEquals(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value, null);
  }

  public static DimFilter and(DimFilter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static DimFilter and(List<DimFilter> filters)
  {
    List<DimFilter> list = Filters.filterNull(filters);
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new AndDimFilter(list);
  }

  public static DimFilter or(DimFilter... filters)
  {
    return or(Arrays.asList(filters));
  }

  public static DimFilter or(List<DimFilter> filters)
  {
    List<DimFilter> list = Filters.filterNull(filters);
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new OrDimFilter(list);
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static RegexDimFilter regex(String dimension, String pattern)
  {
    return new RegexDimFilter(dimension, pattern, null);
  }

  public static List<DimFilter> optimize(List<DimFilter> filters)
  {
    return Lists.newArrayList(
        Lists.transform(
            filters, new Function<DimFilter, DimFilter>()
            {
              @Override
              public DimFilter apply(DimFilter input)
              {
                return input.optimize();
              }
            }
        )
    );
  }

  public static List<DimFilter> filterNulls(List<DimFilter> optimized)
  {
    return Lists.newArrayList(Iterables.filter(optimized, Predicates.notNull()));
  }

  public static DimFilter convertToCNF(DimFilter current)
  {
    return current == null ? null : Expressions.convertToCNF(current, FACTORY);
  }

  public static Pair<ImmutableBitmap, DimFilter> extractBitmaps(DimFilter current, Filters.FilterContext context)
  {
    if (current == null) {
      return Pair.<ImmutableBitmap, DimFilter>of(null, null);
    }
    if (current instanceof AndDimFilter) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      List<DimFilter> remainings = Lists.newArrayList();
      for (DimFilter child : ((AndDimFilter) current).getChildren()) {
        Filters.BitmapHolder holder = Filters.toBitmapHolder(child, context);
        if (holder != null) {
          bitmaps.add(holder.bitmap());
        }
        if (holder == null || !holder.exact()) {
          remainings.add(child);
        }
      }
      ImmutableBitmap extracted = bitmaps.isEmpty() ? null : context.bitmapFactory().intersection(bitmaps);
      return Pair.<ImmutableBitmap, DimFilter>of(extracted, and(remainings));
    } else {
      Filters.BitmapHolder holder = Filters.toBitmapHolder(current, context);
      if (holder != null) {
        return Pair.<ImmutableBitmap, DimFilter>of(holder.bitmap(), holder.exact() ? null : current);
      }
      return Pair.<ImmutableBitmap, DimFilter>of(null, current);
    }
  }

  // should be string type
  public static DimFilter toFilter(String dimension, List<Range> ranges)
  {
    Iterable<Range> filtered = Iterables.filter(ranges, Ranges.VALID);
    List<String> equalValues = Lists.newArrayList();
    List<DimFilter> dimFilters = Lists.newArrayList();
    for (Range range : filtered) {
      String lower = range.hasLowerBound() ? (String) range.lowerEndpoint() : null;
      String upper = range.hasUpperBound() ? (String) range.upperEndpoint() : null;
      if (lower == null && upper == null) {
        return null;
      }
      if (Objects.equals(lower, upper)) {
        equalValues.add(lower);
        continue;
      }
      boolean lowerStrict = range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN;
      boolean upperStrict = range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN;
      dimFilters.add(new BoundDimFilter(dimension, lower, upper, lowerStrict, upperStrict, false, null));
    }
    if (equalValues.size() > 1) {
      dimFilters.add(new InDimFilter(dimension, equalValues, null));
    } else if (equalValues.size() == 1) {
      dimFilters.add(new SelectorDimFilter(dimension, equalValues.get(0), null));
    }
    DimFilter filter = DimFilters.or(dimFilters).optimize();
    LOG.info("Converted dimension '%s' ranges %s to filter %s", dimension, ranges, filter);
    return filter;
  }

  // called for non-historical nodes (see QueryResource.prepareQuery)
  public static Query rewriteLuceneFilter(Query query)
  {
    final DimFilter filter = BaseQuery.getDimFilter(query);
    if (filter != null) {
      DimFilter rewritten = Expressions.rewrite(filter, FACTORY, new Expressions.Rewriter<DimFilter>()
      {
        @Override
        public DimFilter visit(DimFilter expression)
        {
          if (expression instanceof DimFilter.LuceneFilter) {
            expression = ((DimFilter.LuceneFilter) expression).toExpressionFilter();
          }
          return expression;
        }
      });
      if (filter != rewritten) {
        query = ((Query.FilterSupport) query).withFilter(rewritten);
      }
    }
    return query;
  }

  public static boolean hasAnyLucene(final DimFilter filter)
  {
    return filter != null && hasAny(filter, new Predicate<DimFilter>()
    {
      @Override
      public boolean apply(@Nullable DimFilter input)
      {
        return input instanceof DimFilter.LuceneFilter;
      }
    });
  }

  public static boolean hasAny(final DimFilter filter, final Predicate<DimFilter> predicate)
  {
    return Expressions.traverse(
        filter, new Expressions.Visitor<DimFilter, Boolean>()
        {
          private boolean hasAny;

          @Override
          public boolean visit(DimFilter expression)
          {
            return hasAny = predicate.apply(expression);
          }

          @Override
          public Boolean get()
          {
            return hasAny;
          }
        }
    );
  }

  public static DimFilter NONE = new None();

  public static class None extends DimFilter.NotOptimizable
  {
    @Override
    public byte[] getCacheKey()
    {
      return new byte[]{0x7f, 0x00};
    }

    @Override
    public DimFilter withRedirection(Map<String, String> mapping)
    {
      return this;
    }

    @Override
    public void addDependent(Set<String> handler) {}

    @Override
    public Filter toFilter(TypeResolver resolver)
    {
      return Filters.NONE;
    }
  }

  public static DimFilter ALL = new All();

  public static class All extends DimFilter.NotOptimizable
  {
    @Override
    public byte[] getCacheKey()
    {
      return new byte[]{0x7f, 0x01};
    }

    @Override
    public DimFilter withRedirection(Map<String, String> mapping)
    {
      return this;
    }

    @Override
    public void addDependent(Set<String> handler) {}

    @Override
    public Filter toFilter(TypeResolver resolver)
    {
      return new Filter()
      {
        @Override
        public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
        {
          return makeTrue(selector.getBitmapFactory(), selector.getNumRows());
        }

        @Override
        public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
        {
          return baseBitmap != null ? baseBitmap : makeTrue(selector.getBitmapFactory(), selector.getNumRows());
        }

        @Override
        public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
        {
          return ValueMatcher.TRUE;
        }
      };
    }
  }

  public static ImmutableBitmap makeTrue(BitmapFactory factory, int numRows)
  {
    return factory.complement(factory.makeEmptyImmutableBitmap(), numRows);
  }

  public static ImmutableBitmap makeFalse(BitmapFactory factory)
  {
    return factory.makeEmptyImmutableBitmap();
  }
}
