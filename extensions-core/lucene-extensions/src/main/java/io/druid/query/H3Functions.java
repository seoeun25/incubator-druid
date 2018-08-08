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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;

import java.io.IOException;
import java.util.List;

import static io.druid.query.GeoHashFunctions.LATLON;

public class H3Functions implements Function.Library
{
  private static final Supplier<H3Core> H3 = Suppliers.memoize(new Supplier<H3Core>()
  {
    @Override
    public H3Core get()
    {
      try {
        return H3Core.newInstance();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  });

  @Function.Named("to_h3")
  public static class ToH3 extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] must have 3 arguments", name());
      }
      final H3Core instance = H3.get();
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          double latitude = Evals.evalDouble(args.get(0), bindings);
          double longitude = Evals.evalDouble(args.get(1), bindings);
          int precision = Evals.evalInt(args.get(2), bindings);
          return ExprEval.of(instance.geoToH3(latitude, longitude, precision));
        }
      };
    }
  }

  @Function.Named("to_h3_address")
  public static class ToH3Address extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] must have 3 arguments", name());
      }
      final H3Core instance = H3.get();
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          double latitude = Evals.evalDouble(args.get(0), bindings);
          double longitude = Evals.evalDouble(args.get(1), bindings);
          int precision = Evals.evalInt(args.get(2), bindings);
          return ExprEval.of(instance.geoToH3Address(latitude, longitude, precision));
        }
      };
    }
  }

  @Function.Named("h3_to_center")
  public static class H3ToCenter extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      final H3Core instance = H3.get();
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
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          GeoCoord point;
          if (eval.isLong()) {
            point = instance.h3ToGeo(eval.asLong());
          } else {
            point = instance.h3ToGeo(eval.asString());
          }
          return ExprEval.of(new Object[]{point.lat, point.lng}, LATLON);
        }
      };
    }
  }

  @Function.Named("h3_to_boundary")
  public static class H3ToBoundary extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      final H3Core instance = H3.get();
      return new Child()
      {
        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return ValueDesc.DOUBLE_ARRAY;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          List<GeoCoord> points;
          if (eval.isLong()) {
            points = instance.h3ToGeoBoundary(eval.asLong());
          } else {
            points = instance.h3ToGeoBoundary(eval.asString());
          }
          double[] result = new double[points.size() << 1];
          for (int i = 0; i < points.size(); i++) {
            GeoCoord point = points.get(i);
            result[i * 2] = point.lat;
            result[i * 2 + 1] = point.lng;
          }
          return ExprEval.of(result, ValueDesc.DOUBLE_ARRAY);
        }
      };
    }
  }
}