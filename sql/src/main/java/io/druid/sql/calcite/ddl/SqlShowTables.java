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

package io.druid.sql.calcite.ddl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.sql.calcite.schema.InformationSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;

import java.util.List;

public class SqlShowTables
{
  public static SqlNode rewrite(
      SqlParserPos pos,
      SqlIdentifier dbNode,
      SqlNode likePatternNode,
      SqlNode whereNode
  )
  {
    SqlIdentifier tableName = new SqlIdentifier(InformationSchema.TABLE_NAME, SqlParserPos.ZERO);
    List<SqlNode> selectList = ImmutableList.of(tableName);

    SqlNode fromClause = new SqlIdentifier(
        ImmutableList.of(InformationSchema.NAME, InformationSchema.TABLES_TABLE), SqlParserPos.ZERO
    );

    String schema = dbNode != null ? dbNode.toString() : "druid";

    SqlNode where = createCondition(
        new SqlIdentifier(InformationSchema.TABLE_SCHEMA, SqlParserPos.ZERO),
        SqlStdOperatorTable.EQUALS,
        SqlLiteral.createCharString(schema, SqlParserPos.ZERO)
    );

    SqlNode filter = null;
    if (likePatternNode != null) {
      filter = createCondition(tableName, SqlStdOperatorTable.LIKE, likePatternNode);
    } else if (whereNode != null) {
      filter = whereNode;
    }

    if (filter != null) {
      where = createCondition(where, SqlStdOperatorTable.AND, filter);
    }

    return new SqlSelect(
        SqlParserPos.ZERO, null, new SqlNodeList(selectList, SqlParserPos.ZERO),
        fromClause, where, null, null, null, null, null, null
    );
  }

  private static SqlNode createCondition(SqlNode left, SqlOperator op, SqlNode right)
  {
    List<Object> listCondition = Lists.newArrayList();
    listCondition.add(left);
    listCondition.add(new SqlParserUtil.ToTreeListItem(op, SqlParserPos.ZERO));
    listCondition.add(right);

    return SqlParserUtil.toTree(listCondition);
  }
}