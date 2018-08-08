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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.StringUtils;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.Query;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class LuceneGeoJsonPolygonFilter implements DimFilter.LuceneFilter
{
  private final String field;
  private final String geoJson;

  @JsonCreator
  public LuceneGeoJsonPolygonFilter(
      @JsonProperty("field") String field,
      @JsonProperty("polygon") String geoJson
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.geoJson = Preconditions.checkNotNull(geoJson, "geoJson can not be null");
    Preconditions.checkArgument(field.contains("."), "should reference lat-lon point in struct field");
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public String getGeoJson()
  {
    return geoJson;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldBytes = StringUtils.toUtf8(field);
    byte[] geoJsonBytes = StringUtils.toUtf8(geoJson);
    return ByteBuffer.allocate(2 + fieldBytes.length + geoJsonBytes.length)
                     .put(DimFilterCacheHelper.LUCENE_GEOJSON_CACHE_ID)
                     .put(fieldBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(geoJsonBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter()
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(
          BitmapIndexSelector selector,
          EnumSet<BitmapType> using,
          ImmutableBitmap baseBitmap
      )
      {
        // column-name.field-name
        int index = field.indexOf(".");
        String columnName = field.substring(0, index);
        String fieldName = field.substring(index + 1);
        LuceneIndex lucene = Preconditions.checkNotNull(
            selector.getLuceneIndex(columnName),
            "no lucene index for " + columnName
        );
        try {
          Query query = LatLonPoint.newPolygonQuery(fieldName, Polygon.fromGeoJSON(geoJson));
          return lucene.filterFor(query, baseBitmap);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        throw new UnsupportedOperationException("value matcher");
      }

      @Override
      public String toString()
      {
        return LuceneGeoJsonPolygonFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "LuceneGeoJsonPolygonFilter{" +
           "field='" + field + '\'' +
           ", geoJson='" + geoJson + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, geoJson);
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

    LuceneGeoJsonPolygonFilter that = (LuceneGeoJsonPolygonFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!geoJson.equals(that.geoJson)) {
      return false;
    }
    return true;
  }
}