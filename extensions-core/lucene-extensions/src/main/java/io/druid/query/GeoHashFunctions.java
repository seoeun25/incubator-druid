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

package io.druid.query;

import com.metamx.common.IAE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

import java.util.List;

public class GeoHashFunctions implements Function.Library
{
  @Function.Named("to_geohash")
  public static class ToGeoHash extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IAE("Function[%s] must have 2 or 3 arguments", name());
      }
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          double latitude = Evals.evalDouble(args.get(0), bindings);
          double longitude = Evals.evalDouble(args.get(1), bindings);
          if (args.size() == 3) {
            int precision = Evals.evalInt(args.get(2), bindings);
            return ExprEval.of(GeohashUtils.encodeLatLon(latitude, longitude, precision));
          }
          return ExprEval.of(GeohashUtils.encodeLatLon(latitude, longitude));
        }
      };
    }
  }

  public static final ValueDesc LATLON = ValueDesc.of("struct(latitude:double,longitude:double)");

  @Function.Named("geohash_to_center")
  public static class GeoHashToCenter extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new Child()
      {
        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return LATLON;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          Point point = GeohashUtils.decode(Evals.evalString(args.get(0), bindings), JtsSpatialContext.GEO);
          return ExprEval.of(new Object[]{point.getY(), point.getX()}, LATLON);
        }
      };
    }
  }

  public static final ValueDesc BOUNDARY = ValueDesc.of("struct(minLat:double,maxLat:double,minLon:double,maxLon:double)");

  @Function.Named("geohash_to_boundary")
  public static class GeoHashToBoundary extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new Child()
      {
        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return BOUNDARY;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          Rectangle boundary = GeohashUtils.decodeBoundary(
              Evals.evalString(args.get(0), bindings),
              JtsSpatialContext.GEO
          );
          return ExprEval.of(new Object[]{
              boundary.getMinY(),
              boundary.getMaxY(),
              boundary.getMinX(),
              boundary.getMaxX()
          }, BOUNDARY);
        }
      };
    }
  }
}
