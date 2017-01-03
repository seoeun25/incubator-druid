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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.CountingOutputStream;
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
  public static String getFormat(Map<String, Object> context)
  {
    String format = Objects.toString(context.get("format"), "json");
    return Preconditions.checkNotNull(format, "format is null").toLowerCase();
  }

  public static CountingAccumulator toBasicExporter(
      Map<String, Object> context,
      ObjectMapper jsonMapper,
      ByteSink output
  ) throws IOException
  {
    if ("excel".equals(getFormat(context))) {
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
        public CountingAccumulator init()
        {
          Row r = nextRow(true);
          for (int i = 0; i < dimensions.length; i++) {
            Cell c = r.createCell(i);
            c.setCellValue(dimensions[i]);
          }
          return this;
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
            addToCell(r.createCell(i), o);
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
          addToCell(r.createCell(i++), o);
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
    private final CountingOutputStream export;

    private SXSSFSheet sheet;
    private int rowNumInSheet;
    private int rowNum;

    protected ExcelAccumulator(ByteSink sink, int flushInterval, int maxRowsPerSheet) throws IOException
    {
      this.sink = sink;
      this.flushInterval = flushInterval;
      this.maxRowsPerSheet = maxRowsPerSheet > 0 ? Math.min(maxRowsPerSheet, MAX_ROW_INDEX) : MAX_ROW_INDEX;
      this.export = new CountingOutputStream(sink.openBufferedStream());
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

    protected void addToCell(Cell c, Object o)
    {
      if (o instanceof Number) {
        c.setCellValue(((Number) o).doubleValue());
      } else if (o instanceof String) {
        c.setCellValue((String) o);
      } else if (o instanceof Date) {
        c.setCellValue((Date) o);
      } else if (o instanceof Boolean) {
        c.setCellValue((Boolean) o);
      } else {
        c.setCellValue(String.valueOf(o));
      }
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

    public int count()
    {
      return rowNum;
    }

    @Override
    public CountingAccumulator init()
    {
      return this;
    }

    @Override
    public Map<String, Object> close() throws IOException
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
      return ImmutableMap.<String, Object>of(
          "rowCount", count(),
          "data", ImmutableMap.of(sink.toString(), export.getCount()));
    }
  }

  public static CountingAccumulator wrapToExporter(final Formatter formatter)
  {
    return new CountingAccumulator()
    {
      @Override
      public CountingAccumulator init() throws IOException
      {
        return this;
      }

      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        try {
          formatter.write(in);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return null;
      }

      @Override
      public Map<String, Object> close() throws IOException
      {
        return formatter.close();
      }
    };
  }

  private static Formatter toBasicFormatter(ByteSink output, Map<String, Object> context, ObjectMapper jsonMapper)
      throws IOException
  {
    String[] columns = parseStrings(context.get("columns"));
    String format = Formatters.getFormat(context);
    if (format.equalsIgnoreCase("json")) {
      boolean wrapAsList = parseBoolean(context.get("wrapAsList"), false);
      return new Formatter.JsonFormatter(output, jsonMapper, columns, wrapAsList);
    }

    String separator;
    if (format.equalsIgnoreCase("csv")) {
      separator = ",";
    } else if (format.equalsIgnoreCase("tsv")) {
      separator = "\t";
    } else {
      throw new IllegalArgumentException("Unsupported format " + format);
    }
    boolean header = parseBoolean(context.get("withHeader"), false);
    String nullValue = Objects.toString(context.get("nullValue"), null);

    return new Formatter.XSVFormatter(output, jsonMapper, separator, nullValue, columns, header);
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
