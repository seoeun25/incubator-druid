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

package io.druid.query.sketch;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.datasketches.theta.SketchModule;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SketchQueryRunnerTest extends QueryRunnerTestHelper
{
  static final SpecificSegmentsQuerySegmentWalker segmentWalker;

  static final ObjectMapper JSON_MAPPER;

  static {
    ObjectMapper mapper = TestHelper.JSON_MAPPER;
    for (Module module : new SketchModule().getJacksonModules()) {
      mapper = mapper.registerModule(module);
    }
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            if (valueId.equals(QuerySegmentWalker.class.getName())) {
              return segmentWalker;
            } else if (valueId.equals(ExecutorService.class.getName())) {
              return segmentWalker.getExecutor();
            }
            return null;
          }
        }
    );
    JSON_MAPPER = mapper;
  }

  static final SketchQueryQueryToolChest toolChest = new SketchQueryQueryToolChest(
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );

  static {
    QueryRunnerFactoryConglomerate conglomerate = TestIndex.segmentWalker.getQueryRunnerFactoryConglomerate();
    SketchQuery dummy = new SketchQuery(TableDataSource.of("dummy"), null, null, null, null, null, null, null, null);
    if (conglomerate.findFactory(dummy) == null) {
      Map<Class<? extends Query>, QueryRunnerFactory> factoryMap = Maps.newHashMap(
          ((DefaultQueryRunnerFactoryConglomerate) conglomerate).getFactories()
      );
      factoryMap.put(
          SketchQuery.class,
          new SketchQueryRunnerFactory(toolChest, QueryRunnerTestHelper.NOOP_QUERYWATCHER)
      );
      conglomerate = new DefaultQueryRunnerFactoryConglomerate(factoryMap);
    }
    segmentWalker = TestIndex.segmentWalker.withConglomerate(conglomerate).withObjectMapper(JSON_MAPPER);
  }

  public static Object[] array(Object... objects)
  {
    return objects;
  }

  public static List list(Object... objects)
  {
    return Arrays.asList(objects);
  }
}
