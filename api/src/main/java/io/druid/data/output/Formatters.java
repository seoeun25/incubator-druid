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

package io.druid.data.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.metamx.common.logger.Logger;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class Formatters
{
  private static final Logger log = new Logger(Formatter.class);

  public static CountingAccumulator toBasicExporter(
      Map<String, Object> context,
      ObjectMapper jsonMapper,
      ByteSink output
  ) throws IOException
  {
    if ("excel".equals(Objects.toString(context.get("format"), null))) {
      return toExcelExporter(output, context);
    } else {
      return wrapToExporter(toBasicFormatter(output, context, jsonMapper));
    }
  }

  private static final int DEFAULT_FLUSH_INTERVAL = 1000;
  private static final int DEFAULT_MAX_ROWS_PER_SHEET = 0;  // MAX

  public static CountingAccumulator toExcelExporter(final ByteSink sink, final Map<String, Object> context)
      throws IOException
  {
    final String[] dimensions = parseStrings(context.get("columns"));
    final int flushInterval = parseInt(context.get("flushInterval"), DEFAULT_FLUSH_INTERVAL);
    final int maxRowsPerSheet = parseInt(context.get("maxRowsPerSheet"), DEFAULT_MAX_ROWS_PER_SHEET);

    if (dimensions != null) {
      return new ExcelAccumulator(sink, flushInterval, maxRowsPerSheet)
      {
        @Override
        public void nextSheet()
        {
          super.nextSheet();
          Row r = nextRow(true);
          for (int i = 0; i < dimensions.length; i++) {
            Cell c = r.createCell(i);
            c.setCellValue(dimensions[i]);
          }
        }

        @Override
        public Void accumulate(Void accumulated, Map<String, Object> in)
        {
          Row r = nextRow(false);
          for (int i = 0; i < dimensions.length; i++) {
            Object o = in.get(dimensions[i]);
            if (o == null) {
              continue;
            }
            Cell c = r.createCell(i);
            if (o instanceof Number) {
              c.setCellValue(((Number) o).doubleValue());
            } else if (o instanceof String) {
              c.setCellValue((String) o);
            } else if (o instanceof Date) {
              c.setCellValue((Date) o);
            } else {
              c.setCellValue(String.valueOf(o));
            }
          }
          flushIfNeeded();
          return null;
        }
      };
    }
    return new ExcelAccumulator(sink, flushInterval, maxRowsPerSheet)
    {
      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        Row r = nextRow(false);
        int i = 0;
        for (Object o : in.values()) {
          Cell c = r.createCell(i++);
          if (o instanceof Number) {
            c.setCellValue(((Number) o).doubleValue());
          } else if (o instanceof String) {
            c.setCellValue((String) o);
          } else if (o instanceof Date) {
            c.setCellValue((Date) o);
          } else {
            c.setCellValue(String.valueOf(o));
          }
        }
        flushIfNeeded();
        return null;
      }
    };
  }

  private static abstract class ExcelAccumulator implements CountingAccumulator
  {
    private static final int MAX_ROW_INDEX = SpreadsheetVersion.EXCEL2007.getLastRowIndex();

    private final ByteSink sink;
    private final int flushInterval;
    private final int maxRowsPerSheet;
    private final SXSSFWorkbook wb = new SXSSFWorkbook(-1);

    private SXSSFSheet sheet;
    private int rowNumInSheet;
    private int rowNum;

    protected ExcelAccumulator(ByteSink sink, int flushInterval, int maxRowsPerSheet)
    {
      this.sink = sink;
      this.flushInterval = flushInterval;
      this.maxRowsPerSheet = maxRowsPerSheet > 0 ? Math.min(maxRowsPerSheet, MAX_ROW_INDEX) : MAX_ROW_INDEX;
    }

    protected Row nextRow(boolean header)
    {
      if (sheet == null || rowNumInSheet >= maxRowsPerSheet) {
        nextSheet();
      }
      Row r = sheet.createRow(rowNumInSheet++);
      if (!header) {
        rowNum++;
      }
      return r;
    }

    protected void flush()
    {
      try {
        sheet.flushRows();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    protected void flushIfNeeded()
    {
      if (rowNumInSheet % flushInterval == 0) {
        flush();
      }
    }

    protected void nextSheet()
    {
      sheet = wb.createSheet();
      rowNumInSheet = 0;
    }

    @Override
    public int count()
    {
      return rowNum;
    }

    @Override
    public void init() {
    }

    @Override
    public void close() throws IOException
    {
      try {
        try (OutputStream output = sink.openBufferedStream()) {
          wb.write(output);
        }
      }
      finally {
        wb.dispose();
        wb.close();
      }
    }
  }

  public static CountingAccumulator wrapToExporter(final Formatter formatter)
  {
    return new CountingAccumulator()
    {
      int counter = 0;

      @Override
      public int count()
      {
        return counter;
      }

      @Override
      public void init() throws IOException
      {
      }

      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        try {
          formatter.write(in);
          counter++;
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return null;
      }

      @Override
      public void close() throws IOException
      {
        formatter.close();
      }
    };
  }

  public static Formatter toBasicFormatter(ByteSink output, Map<String, Object> context, ObjectMapper jsonMapper)
      throws IOException
  {
    String[] columns = parseStrings(context.get("columns"));
    String formatString = Objects.toString(context.get("format"), null);
    if (isNullOrEmpty(formatString) || formatString.equalsIgnoreCase("json")) {
      boolean wrapAsList = parseBoolean(context.get("wrapAsList"), false);
      return new Formatter.JsonFormatter(output.openBufferedStream(), jsonMapper, columns, wrapAsList);
    }

    String separator;
    if (formatString.equalsIgnoreCase("csv")) {
      separator = ",";
    } else if (formatString.equalsIgnoreCase("tsv")) {
      separator = "\t";
    } else {
      log.warn("Invalid format " + formatString + ".. using json formatter instead");
      boolean wrapAsList = parseBoolean(context.get("wrapAsList"), false);
      return new Formatter.JsonFormatter(output.openBufferedStream(), jsonMapper, columns, wrapAsList);
    }
    boolean header = parseBoolean(context.get("withHeader"), false);
    String nullValue = Objects.toString(context.get("nullValue"), null);

    return new Formatter.XSVFormatter(output.openBufferedStream(), jsonMapper, separator, nullValue, columns, header);
  }

  private static boolean isNullOrEmpty(String string)
  {
    return string == null || string.isEmpty();
  }

  private static boolean parseBoolean(Object input, boolean defaultValue)
  {
    return input == null ? defaultValue :
           input instanceof Boolean ? (Boolean) input : Boolean.valueOf(String.valueOf(input));
  }

  private static int parseInt(Object input, int defaultValue)
  {
    return input == null ? defaultValue :
           input instanceof Number ? ((Number) input).intValue() : Integer.valueOf(String.valueOf(input));
  }

  private static String[] parseStrings(Object input)
  {
    if (input instanceof List) {
      List<String> stringList = Lists.transform((List<?>) input, Functions.toStringFunction());
      return stringList.toArray(new String[stringList.size()]);
    }
    String stringVal = Objects.toString(input, null);
    if (isNullOrEmpty(stringVal)) {
      return null;
    }
    return Iterables.toArray(
        Iterables.transform(
            Arrays.asList(stringVal.split(",")), new Function<String, String>()
            {
              @Override
              public String apply(String input)
              {
                return input.trim();
              }
            }
        ),
        String.class
    );
  }
}