/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlDialect</code> implementation for the Clickhouse database.
 */
public class ClickhouseSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new ClickhouseSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.MYSQL)
          .withIdentifierQuoteString("`")
          .withNullCollation(NullCollation.LOW));

  /** Creates a ClickhouseSqlDialect. */
  public ClickhouseSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsOffsetFetch() {
    return false;
  }

  // ???
  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  // ???
  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case COUNT:
    case SUM:
    case SUM0:
    case MIN:
    case MAX:
    case SINGLE_VALUE:
      return true;
    }
    return false;
  }

  // ???
  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  // ???
  @Override public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.SHIFT;
  }

  // ???
  @Override public SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case VARCHAR:
      // MySQL doesn't have a VARCHAR type, only String.
      return new SqlDataTypeSpec(new SqlIdentifier("String", SqlParserPos.ZERO),
          type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
    case INTEGER:
      return new SqlDataTypeSpec(new SqlIdentifier("Int32", SqlParserPos.ZERO),
          type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
    }
    return super.getCastSpec(type);
  }

  // ???
  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    final SqlNode unionOperand = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
        SqlNodeList.of(nullLiteral), null, null, null, null, SqlNodeList.EMPTY, null, null, null);
    // For MySQL, generate
    //   CASE COUNT(*)
    //   WHEN 0 THEN NULL
    //   WHEN 1 THEN <result>
    //   ELSE (SELECT NULL UNION ALL SELECT NULL)
    //   END
    final SqlNode caseExpr =
        new SqlCase(SqlParserPos.ZERO,
            SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand),
            SqlNodeList.of(
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
            ),
            SqlNodeList.of(
                nullLiteral,
                operand
            ),
            SqlStdOperatorTable.SCALAR_QUERY.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.UNION_ALL
                    .createCall(SqlParserPos.ZERO, unionOperand, unionOperand)));

    LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);

    return caseExpr;
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    String formattedLit;
    if (literal instanceof SqlDateLiteral) {
      formattedLit = "toDate('" + literal.toFormattedString() + "')";
    } else if (literal instanceof SqlTimestampLiteral) {
      formattedLit = "toDateTime('" + literal.toFormattedString() + "')";
    } else if (literal instanceof SqlTimeLiteral) {
      formattedLit = "toTime('" + literal.toFormattedString() + "')";
    } else {
      throw new AssertionError("Clickhouse does not support DateTime literal: "
          + literal);
    }

    writer.literal(formattedLit);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      unparseFloor(writer, call);
      break;

    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * Unparses datetime floor for Clickhouse.
   *
   * @param writer Writer
   * @param call Call
   */
  private void unparseFloor(SqlWriter writer, SqlCall call) {
    final SqlLiteral timeUnitNode = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) timeUnitNode.getValue();

    String funName;
    switch (unit) {
    case YEAR:
      funName = "toStartOfYear";
      break;
    case MONTH:
      funName = "toStartOfMonth";
      break;
    case WEEK:
      funName = "toMonday";
      break;
    case DAY:
      funName = "toDate";
      break;
    case HOUR:
      funName = "toStartOfHour";
      break;
    case MINUTE:
      funName = "toStartOfMinute";
      break;
    default:
      throw new AssertionError("Clickhouse does not support FLOOR for time unit: "
          + unit);
    }

    writer.print(funName);
    SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, 0, 0);
    writer.endList(frame);
  }
}

// End ClickhouseSqlDialect.java
