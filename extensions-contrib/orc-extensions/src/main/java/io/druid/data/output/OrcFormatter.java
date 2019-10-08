/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.output;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.data.Rows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * to validate, use hive --orcfiledump -d path
 */
@JsonTypeName("orc")
public class OrcFormatter implements Formatter
{
  private static final Logger log = new Logger(OrcFormatter.class);

  private final String typeString;
  private final ObjectMapper mapper;
  private final List<String> inputColumns;
  private final List<TypeDescription> outputSchema;

  private final Path path;
  private final FileSystem fs;
  private final Writer writer;
  private final VectorizedRowBatch batch;

  private int counter;

  @JsonCreator
  public OrcFormatter(
      @JsonProperty("outputPath") String outputPath,
      @JsonProperty("inputColumns") String[] inputColumns,
      @JsonProperty("typeString") String typeString,
      @JacksonInject ObjectMapper mapper
  ) throws IOException
  {
    log.info("Applying schema : %s", typeString);
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(OrcFormatter.class.getClassLoader());
    try {
      Configuration conf = new Configuration();
      TypeDescription schema = TypeDescriptions.fromString(TypeDescriptions.toOrcTypeString(typeString));
      this.path = new Path(outputPath);
      this.inputColumns = inputColumns == null ? schema.getFieldNames() : Arrays.asList(inputColumns);
      this.outputSchema = schema.getChildren();
      this.writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema));
      this.batch = schema.createRowBatch();
      this.fs = path.getFileSystem(conf);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
    this.mapper = mapper;
    this.typeString = typeString;
  }

  @Override
  public void write(Map<String, Object> datum) throws IOException
  {
    final int rowId = batch.size++;
    for (int i = 0; i < inputColumns.size(); i++) {
      Object object = datum.get(inputColumns.get(i));
      setColumn(rowId, object, batch.cols[i], outputSchema.get(i));
    }
    if (batch.size == batch.getMaxSize()) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    counter++;
  }

  private void setColumn(int rowId, Object object, ColumnVector vector, TypeDescription column)
      throws JsonProcessingException
  {
    if (object == null) {
      setNull(vector, rowId);
      return;
    }
    switch (column.getCategory()) {
      case INT:
      case LONG:
        final Long longVal = Rows.parseLong(object);
        if (longVal != null) {
          ((LongColumnVector) vector).vector[rowId] = longVal;
          return;
        }
        break;
      case FLOAT:
      case DOUBLE:
        final Double doubleVal = Rows.parseDouble(object);
        if (doubleVal != null) {
          ((DoubleColumnVector) vector).vector[rowId] = doubleVal;
          return;
        }
        break;
      case DECIMAL:
        if (object instanceof BigDecimal) {
          ((DecimalColumnVector) vector).vector[rowId] =
              new HiveDecimalWritable(HiveDecimal.create((BigDecimal) object));
        }
        return;
      case STRING:
        ((BytesColumnVector) vector).setVal(rowId, StringUtils.toUtf8(object.toString()));
        return;
      case BINARY:
        byte[] b = object instanceof byte[] ? (byte[]) object : mapper.writeValueAsBytes(object);
        ((BytesColumnVector) vector).setVal(rowId, b, 1, b.length - 1);
        return;
      case LIST:
        final ListColumnVector list = (ListColumnVector) vector;
        final TypeDescription elementType = column.getChildren().get(0);
        final ColumnVector elements = list.child;
        final int offset = list.childCount;

        final int length;
        if (object instanceof List) {
          final List values = (List) object;
          length = values.size();
          elements.ensureSize(offset + length, true);
          for (int j = 0; j < length; j++) {
            setColumn(offset + j, values.get(j), elements, elementType);
          }
        } else if (object.getClass().isArray()) {
          length = Array.getLength(object);
          elements.ensureSize(offset + length, true);
          for (int j = 0; j < length; j++) {
            setColumn(offset + j, Array.get(object, j), elements, elementType);
          }
        } else {
          length = 1;
          elements.ensureSize(offset + length, true);
          setColumn(offset, object, elements, elementType);
        }
        list.offsets[rowId] = offset;
        list.lengths[rowId] = length;
        list.childCount += length;
        return;
      default:
        throw new UnsupportedOperationException("Not supported type " + column.getCategory());
    }
    setNull(vector, rowId);
  }

  private void setNull(ColumnVector vector, int index)
  {
    vector.isNull[index] = true;
    vector.noNulls = false;
  }

  @Override
  public Map<String, Object> close() throws IOException
  {
    if (batch.size > 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    long length = fs.getFileStatus(path).getLen();
    return ImmutableMap.<String, Object>of(
        "rowCount", counter,
        "typeString", typeString,
        "data", ImmutableMap.of(path.toString(), length)
    );
  }
}