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

package io.druid.segment.indexing;

import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.segment.column.Column;

import java.util.Collection;
import java.util.Map;

/**
 */
public class RowBinding<T> implements Expr.NumericBinding
{
  private final String defaultColumn;
  private final Map<String, ValueType> types;

  private volatile Row row;
  private volatile boolean evaluated;
  private volatile T tempResult;

  public RowBinding(String defaultColumn, Map<String, ValueType> types)
  {
    this.defaultColumn = defaultColumn;
    this.types = types;
  }

  @Override
  public Collection<String> names()
  {
    return row.getColumns();
  }

  @Override
  public Object get(String name)
  {
    if (Column.TIME_COLUMN_NAME.equals(name)) {
      return row.getTimestampFromEpoch();
    }
    if (!name.equals("_")) {
      return getColumn(name);
    }
    return evaluated ? tempResult : getColumn(defaultColumn);
  }

  private Object getColumn(String name)
  {
    ValueType type = types.get(name);
    return type == null ? row.getRaw(name) : type.get(row, name);
  }

  public void reset(Row row)
  {
    this.row = row;
    this.evaluated = false;
    this.tempResult = null;
  }

  public void set(T eval)
  {
    this.evaluated = true;
    this.tempResult = eval;
  }

  public T get()
  {
    return tempResult;
  }
}
