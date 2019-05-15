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

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.Pair;
import io.druid.data.ValueDesc;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public interface PredicateFunctions extends Function.Library
{
  @Function.Named("isNull")
  final class IsNullFunc extends BuiltinFunctions.SingleParam
  {
    @Override
    public ValueDesc type(ValueDesc param)
    {
      return ValueDesc.BOOLEAN;
    }

    @Override
    public ExprEval eval(ExprEval param)
    {
      return ExprEval.of(param.isNull());
    }
  }

  @Function.Named("isNotNull")
  final class IsNotNullFunc extends BuiltinFunctions.SingleParam
  {
    @Override
    public ValueDesc type(ValueDesc param)
    {
      return ValueDesc.BOOLEAN;
    }

    @Override
    public ExprEval eval(ExprEval param)
    {
      return ExprEval.of(!param.isNull());
    }
  }

  @Function.Named("IsTrue")
  final class IsTrue extends BuiltinFunctions.SingleParam
  {
    @Override
    public ValueDesc type(ValueDesc param)
    {
      return ValueDesc.BOOLEAN;
    }

    @Override
    public ExprEval eval(ExprEval param)
    {
      return ExprEval.of(param.asBoolean());
    }
  }

  @Function.Named("isFalse")
  final class isFalse extends BuiltinFunctions.SingleParam
  {
    @Override
    public ValueDesc type(ValueDesc param)
    {
      return ValueDesc.BOOLEAN;
    }

    @Override
    public ExprEval eval(ExprEval param)
    {
      return ExprEval.of(!param.asBoolean());
    }
  }

  @Function.Named("like")
  final class Like extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 2 arguments");
      }
      final Pair<RegexUtils.PatternType, Object> matcher = RegexUtils.parse(Evals.getConstantString(args.get(1)));
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = args.get(0).eval(bindings);
          return ExprEval.of(RegexUtils.evaluate(eval.asString(), matcher.lhs, matcher.rhs));
        }
      };
    }
  }

  @Function.Named("in")
  final class InFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'in' needs at least 2 arguments");
      }
      final Set<Object> set = Sets.newHashSet();
      for (int i = 1; i < args.size(); i++) {
        set.add(Evals.getConstant(args.get(i)));
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(set.contains(args.get(0).eval(bindings).value()));
        }
      };
    }
  }

  @Function.Named("between")
  final class BetweenFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'between' needs 3 arguments");
      }
      ExprEval eval1 = Evals.getConstantEval(args.get(1));
      ExprEval eval2 = Evals.castTo(Evals.getConstantEval(args.get(2)), eval1.type());
      final Range<Comparable> range = Range.closed((Comparable) eval1.value(), (Comparable) eval2.value());
      final ValueDesc type = eval1.type();
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = Evals.castTo(args.get(0).eval(bindings), type);
          return ExprEval.of(range.contains((Comparable) eval.value()));
        }
      };
    }
  }

  @Function.Named("startsWith")
  final class StartsWithFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'startsWith' needs 2 arguments");
      }
      final String prefix = Evals.getConstantString(args.get(1));
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.startsWith(prefix));
        }
      };
    }
  }

  @Function.Named("endsWith")
  final class EndsWithFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'endsWith' needs 2 arguments");
      }
      final String suffix = Evals.getConstantString(args.get(1));
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? suffix == null : suffix != null && eval.endsWith(suffix));
        }
      };
    }
  }

  @Function.Named("startsWithIgnoreCase")
  final class StartsWithIgnoreCaseFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'startsWithIgnoreCase' needs 2 arguments");
      }
      String value = Evals.getConstantString(args.get(1));
      final String prefix = value == null ? null : value.toLowerCase();
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.toLowerCase().startsWith(prefix));
        }
      };
    }
  }

  @Function.Named("endsWithIgnoreCase")
  final class EndsWithIgnoreCaseFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'endsWithIgnoreCase' needs 2 arguments");
      }
      String value = Evals.getConstantString(args.get(1));
      final String suffix = value == null ? null : value.toLowerCase();
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? suffix == null : suffix != null && eval.toLowerCase().endsWith(suffix));
        }
      };
    }
  }

  @Function.Named("contains")
  final class ContainsFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'contains' needs 2 arguments");
      }
      final String contained = Evals.getConstantString(args.get(1));
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? contained == null : contained != null && eval.contains(contained));
        }
      };
    }
  }

  @Function.Named("match")
  final class MatchFunc extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'match' needs 2 arguments");
      }
      final Matcher matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval != null && matcher.reset(eval).find());
        }
      };
    }
  }

  @Function.Named("ipv4_in")
  final class IPv4In extends Function.BooleanFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'ipv4_in' needs at least 2 arguments");
      }
      final byte[] start = InetAddresses.forString(Evals.getConstantString(args.get(1))).getAddress();
      final byte[] end;
      Preconditions.checkArgument(start.length == 4);
      if (args.size() > 2) {
        end = InetAddresses.forString(Evals.getConstantString(args.get(2))).getAddress();
        Preconditions.checkArgument(end.length == 4);
      } else {
        end = Ints.toByteArray(-1);
      }
      for (int i = 0; i < 4; i++) {
        if (UnsignedBytes.compare(start[i], end[i]) > 0) {
          throw new IllegalArgumentException("start[n] <= end[n]");
        }
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String ipString = Evals.evalString(args.get(0), bindings);
          try {
            return ExprEval.of(evaluate(ipString));
          }
          catch (Exception e) {
            return ExprEval.of(false);
          }
        }

        private boolean evaluate(String ipString)
        {
          final byte[] address = InetAddresses.forString(ipString).getAddress();
          if (address.length != 4) {
            return false;
          }
          for (int i = 0; i < 4; i++) {
            if (UnsignedBytes.compare(address[i], start[i]) < 0 || UnsignedBytes.compare(address[i], end[i]) > 0) {
              return false;
            }
          }
          return true;
        }
      };
    }
  }
}
