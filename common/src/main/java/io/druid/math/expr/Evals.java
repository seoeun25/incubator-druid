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

package io.druid.math.expr;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.metamx.common.Pair;
import io.druid.common.utils.JodaUtils;
import io.druid.data.ValueType;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 */
public class Evals
{
  static final DateTimeFormatter defaultFormat = JodaUtils.toTimeFormatter("yyyy-MM-dd HH:mm:ss[.SSSSSS]");

  public static final Predicate<ExprEval> PREDICATE = new Predicate<ExprEval>()
  {
    @Override
    public boolean apply(ExprEval input)
    {
      return input.asBoolean();
    }
  };

  public static final com.google.common.base.Function<ExprEval, String> AS_STRING =
      new com.google.common.base.Function<ExprEval, String>()
      {
        @Override
        public String apply(ExprEval input)
        {
          return input.asString();
        }
      };

  public static final com.google.common.base.Function<ExprEval, Double> AS_DOUBLE =
      new com.google.common.base.Function<ExprEval, Double>()
      {
        @Override
        public Double apply(ExprEval input)
        {
          return input.asDouble();
        }
      };

  public static final com.google.common.base.Function<ExprEval, Long> AS_LONG =
      new com.google.common.base.Function<ExprEval, Long>()
      {
        @Override
        public Long apply(ExprEval input)
        {
          return input.asLong();
        }
      };

  static boolean eq(ExprEval leftVal, ExprEval rightVal)
  {
    if (isSameType(leftVal, rightVal)) {
      return Objects.equals(leftVal.value(), rightVal.value());
    }
    if (isAllNumeric(leftVal, rightVal)) {
      return leftVal.doubleValue() == rightVal.doubleValue();
    }
    return false;
  }

  private static boolean isSameType(ExprEval leftVal, ExprEval rightVal)
  {
    return leftVal.type() == rightVal.type();
  }

  static boolean isAllNumeric(ExprEval left, ExprEval right)
  {
    return left.isNumeric() && right.isNumeric();
  }

  static boolean isAllString(ExprEval left, ExprEval right)
  {
    return left.type() == ExprType.STRING && right.type() == ExprType.STRING;
  }

  static void assertNumeric(ExprType type)
  {
    if (type != ExprType.LONG && type != ExprType.DOUBLE) {
      throw new IllegalArgumentException("unsupported type " + type);
    }
  }

  static String evalOptionalString(Expr arg, Expr.NumericBinding binding)
  {
    return arg == null ? null : arg.eval(binding).asString();
  }

  static String getConstantString(Expr arg)
  {
    if (!(arg instanceof StringExpr)) {
      throw new RuntimeException(arg + " is not constant string");
    }
    return arg.eval(null).stringValue();
  }

  public static boolean isIdentifier(Expr arg)
  {
    return arg instanceof IdentifierExpr;
  }

  public static String getIdentifier(Expr arg)
  {
    if (!isIdentifier(arg)) {
      throw new RuntimeException(arg + " is not identifier");
    }
    return arg.toString();
  }

  static long getConstantLong(Expr arg)
  {
    Object constant = getConstant(arg);
    if (!(constant instanceof Long)) {
      throw new RuntimeException(arg + " is not a constant long");
    }
    return (Long) constant;
  }

  static Number getConstantNumber(Expr arg)
  {
    Object constant = getConstant(arg);
    if (!(constant instanceof Number)) {
      throw new RuntimeException(arg + " is not a constant number");
    }
    return (Number) constant;
  }

  static Object getConstant(Expr arg)
  {
    if (arg instanceof StringExpr) {
      return arg.eval(null).stringValue();
    } else if (arg instanceof LongExpr) {
      return arg.eval(null).longValue();
    } else if (arg instanceof DoubleExpr) {
      return arg.eval(null).doubleValue();
    } else if (arg instanceof UnaryMinusExpr) {
      UnaryMinusExpr minusExpr = (UnaryMinusExpr)arg;
      if (minusExpr.expr instanceof LongExpr) {
        return -minusExpr.expr.eval(null).longValue();
      } else if (minusExpr.expr instanceof DoubleExpr) {
        return -minusExpr.expr.eval(null).doubleValue();
      }
    }
    throw new RuntimeException(arg + " is not a constant");
  }

  static boolean isConstantString(Expr arg)
  {
    return arg instanceof StringExpr;
  }

  static boolean isConstant(Expr arg)
  {
    if (arg instanceof Constant) {
      return true;
    } else if (arg instanceof UnaryMinusExpr) {
      return ((UnaryMinusExpr)arg).expr instanceof Constant;
    }
    return false;
  }

  static Object[] getConstants(List<Expr> args)
  {
    Object[] constants = new Object[args.size()];
    for (int i = 0; i < constants.length; i++) {
      constants[i] = getConstant(args.get(i));
    }
    return constants;
  }

  static ExprEval castTo(ExprEval eval, ExprType castTo)
  {
    if (eval.type() == castTo) {
      return eval;
    }
    switch (castTo) {
      case DOUBLE:
        return ExprEval.of(eval.asDouble());
      case LONG:
        return ExprEval.of(eval.asLong());
      case STRING:
        return ExprEval.of(eval.asString());
      case DATETIME:
        return ExprEval.of(eval.asDateTime());
    }
    throw new IllegalArgumentException("not supported type " + castTo);
  }

  public static Object castTo(ExprEval eval, ValueType castTo)
  {
    switch (castTo) {
      case FLOAT:
        return eval.asFloat();
      case DOUBLE:
        return eval.asDouble();
      case LONG:
        return eval.asLong();
      case STRING:
        return eval.asString();
      default:
        throw new IllegalArgumentException("not supported type " + castTo);
    }
  }

  public static com.google.common.base.Function<Comparable, Number> asNumberFunc(ValueType type)
  {
    switch (type) {
      case FLOAT:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0F : (Float) input;
          }
        };
      case DOUBLE:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0D : (Double) input;
          }
        };
      case LONG:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0L : (Long) input;
          }
        };
      case STRING:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            String string = (String) input;
            return Strings.isNullOrEmpty(string)
                   ? 0L
                   : StringUtils.isNumeric(string) ? Long.valueOf(string) : Double.valueOf(string);
          }
        };
    }
    throw new UnsupportedOperationException("Unsupported type " + type);
  }

  static DateTime toDateTime(ExprEval arg, String timeZone)
  {
    switch (arg.type()) {
      case DATETIME:
        return arg.asDateTime();
      case LONG:
        return new DateTime(arg.asLong(), DateTimeZone.forID(timeZone));
      case STRING:
        final String string = arg.stringValue();
        if (StringUtils.isNumeric(string)) {
          return new DateTime(Long.valueOf(string), DateTimeZone.forID(timeZone));
        } else {
          return timeZone == null
                 ? defaultFormat.parseDateTime(string)
                 : defaultFormat.withZone(DateTimeZone.forID(timeZone)).parseDateTime(string);
        }
      default:
        return new DateTime(arg.longValue(), DateTimeZone.forID(timeZone));
    }
  }

  public static Pair<String, Expr> splitAssign(String expression)
  {
    Expr expr = Parser.parse(expression);
    if (!(expr instanceof AssignExpr)) {
      List<String> required = Parser.findRequiredBindings(expr);
      if (required.size() != 1) {
        throw new RuntimeException("cannot resolve output column " + expression);
      }
      return Pair.of(required.get(0), expr);
    }
    final AssignExpr assign = (AssignExpr) expr;
    Expr.NumericBinding bindings = new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return Parser.findRequiredBindings(assign.assignee);
      }

      @Override
      public Object get(String name)
      {
        return name;
      }
    };
    return Pair.of(assign.assignee.eval(bindings).stringValue(), assign.assigned);
  }
}
