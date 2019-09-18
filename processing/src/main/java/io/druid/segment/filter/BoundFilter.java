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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueType;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.BitmapIndex;

import java.util.Comparator;
import java.util.Iterator;

public class BoundFilter implements Filter
{
  private final BoundDimFilter boundDimFilter;

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
  }

  @Override
  public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
  {
    if (boundDimFilter.getExtractionFn() != null) {
      return null;
    }
    BitmapFactory factory = selector.getBitmapFactory();
    final int[] range = toRange(selector.getBitmapIndex(boundDimFilter.getDimension()));
    if (range == ALL) {
      return DimFilters.makeTrue(factory, selector.getNumRows());
    } else if (range == NONE) {
      return DimFilters.makeFalse(factory);
    }
    final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    for (int i = range[0]; i < range[1]; i++) {
      bitmap.add(i);
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
  {
    // asserted to existing dimension
    if (boundDimFilter.isLexicographic() && boundDimFilter.getExtractionFn() == null) {
      return toRangeBitmap(selector, boundDimFilter.getDimension());
    }
    return Filters.matchPredicate(
        boundDimFilter.getDimension(),
        selector,
        toPredicate(
            boundDimFilter.typeOfBound(Filters.asTypeResolver(selector)),
            boundDimFilter.getExtractionFn()
        )
    );
  }

  private static final int[] ALL = new int[]{0, Integer.MAX_VALUE};
  private static final int[] NONE = new int[]{0, 0};

  // can be slower..
  private ImmutableBitmap toRangeBitmap(BitmapIndexSelector selector, String dimension)
  {
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    final int[] range = toRange(bitmapIndex);

    final BitmapFactory factory = selector.getBitmapFactory();
    if (range == ALL) {
      return DimFilters.makeTrue(factory, selector.getNumRows());
    } else if (range == NONE) {
      return DimFilters.makeFalse(factory);
    }

    // search for start, end indexes in the bitmaps; then include all bitmaps between those points

    final int startIndex = range[0];
    final int endIndex = range[1];

    return factory.union(
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              int currIndex = startIndex;

              @Override
              public boolean hasNext()
              {
                return currIndex < endIndex;
              }

              @Override
              public ImmutableBitmap next()
              {
                return bitmapIndex.getBitmap(currIndex++);
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );
  }

  @SuppressWarnings("unchecked")
  private int[] toRange(BitmapIndex bitmapIndex)
  {
    if (bitmapIndex == null || bitmapIndex.getCardinality() == 0) {
      return toPredicate(ValueType.STRING, boundDimFilter.getExtractionFn()).apply(null) ? ALL : NONE;
    }
    final String lower = Strings.emptyToNull(boundDimFilter.getLower());
    final String upper = Strings.emptyToNull(boundDimFilter.getUpper());

    if (lower == null && upper == null) {
      return Strings.isNullOrEmpty(bitmapIndex.getValue(0)) ? new int[] {0, 1} : NONE;
    }
    // search for start, end indexes in the bitmaps; then include all bitmaps between those points

    final int startIndex; // inclusive
    final int endIndex; // exclusive

    if (lower == null) {
      startIndex = boundDimFilter.isLowerStrict() ? 1 : 0;
    } else {
      final int found = bitmapIndex.getIndex(lower);
      if (found >= 0) {
        startIndex = boundDimFilter.isLowerStrict() ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (upper == null) {
      endIndex = bitmapIndex.getCardinality();
    } else {
      final int found = bitmapIndex.getIndex(upper);
      if (found >= 0) {
        endIndex = boundDimFilter.isUpperStrict() ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }
    return new int[]{startIndex, endIndex};
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.toValueMatcher(
        factory,
        boundDimFilter.getDimension(),
        toPredicate(
            boundDimFilter.typeOfBound(ColumnSelectorFactories.asTypeResolver(factory)),
            boundDimFilter.getExtractionFn()
        )
    );
  }

  private Predicate toPredicate(ValueType type, Function extractionFn)
  {
    if (extractionFn == null) {
      extractionFn = type == ValueType.STRING ? GuavaUtils.NULLABLE_TO_STRING_FUNC : Functions.identity();
    }
    String lower = Strings.emptyToNull(boundDimFilter.getLower());
    String upper = Strings.emptyToNull(boundDimFilter.getUpper());

    Comparable lowerLimit = lower != null ? type.cast(boundDimFilter.getLower()) : null;
    Comparable upperLimit = upper != null ? type.cast(boundDimFilter.getUpper()) : null;

    Comparator comparator = type == ValueType.STRING ? boundDimFilter.getComparator() : type.comparator();
    return asPredicate(lowerLimit, upperLimit, extractionFn, comparator);
  }

  private Predicate asPredicate(
      final Comparable lower,
      final Comparable upper,
      final Function extractionFn,
      final Comparator comparator
  )
  {
    final boolean hasLowerBound = lower != null;
    final boolean lowerStrict = boundDimFilter.isLowerStrict();

    final boolean hasUpperBound = upper != null;
    final boolean upperStrict = boundDimFilter.isUpperStrict();

    // lower bound allows null && upper bound allows null
    final boolean allowNull = lower == null && !lowerStrict || upper == null && !upperStrict;
    final boolean allowOnlyNull = allowNull && lower == null && upper == null;

    return new Predicate()
    {
      @Override
      @SuppressWarnings("unchecked")
      public boolean apply(final Object input)
      {
        Object value = extractionFn.apply(input);
        if (value == null) {
          return allowNull;
        } else if (allowOnlyNull) {
          return false;
        }
        int lowerComparing = 1;
        int upperComparing = 1;
        if (hasLowerBound) {
          lowerComparing = comparator.compare(value, lower);
        }
        if (hasUpperBound) {
          upperComparing = comparator.compare(upper, value);
        }
        if (lowerStrict && upperStrict) {
          return lowerComparing > 0 && upperComparing > 0;
        } else if (lowerStrict) {
          return lowerComparing > 0 && upperComparing >= 0;
        } else if (upperStrict) {
          return lowerComparing >= 0 && upperComparing > 0;
        }
        return lowerComparing >= 0 && upperComparing >= 0;
      }
    };
  }
}
