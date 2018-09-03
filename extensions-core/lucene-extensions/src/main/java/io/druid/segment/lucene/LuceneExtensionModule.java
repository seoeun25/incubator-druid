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

package io.druid.segment.lucene;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.query.ChoroplethMapQuery;
import io.druid.query.GeoHashFunctions;
import io.druid.query.H3Functions;
import io.druid.query.filter.LuceneLatLonPolygonFilter;
import io.druid.query.filter.LuceneSpatialFilter;

import java.util.List;

public class LuceneExtensionModule implements DruidModule
{
  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("lucene-extension")
            .registerSubtypes(SpatialIndexingStrategy.class)
            .registerSubtypes(LuceneSpatialFilter.class)
            .registerSubtypes(LuceneLatLonPolygonFilter.class)
            .registerSubtypes(GeoHashFunctions.class)
            .registerSubtypes(H3Functions.class)
            .registerSubtypes(ChoroplethMapQuery.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
