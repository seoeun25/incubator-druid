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

package io.druid.segment;

import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.common.utils.SerializerUtils;
import io.druid.data.ValueType;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;
import io.druid.segment.data.CompressedDoublesSupplierSerializer;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedFloatsSupplierSerializer;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedLongsSupplierSerializer;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedDoubles;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class MetricHolder
{
  private static final byte[] version = new byte[]{0x0};
  private static final SerializerUtils serializerUtils = new SerializerUtils();

  public static void writeComplexMetric(
      OutputSupplier<? extends OutputStream> outSupplier, String name, String typeName, GenericIndexedWriter column
  ) throws IOException
  {
    try (OutputStream out = outSupplier.getOutput()) {
      out.write(version);
      serializerUtils.writeString(out, name);
      serializerUtils.writeString(out, typeName);

      final InputSupplier<InputStream> supplier = column.combineStreams();
      try (InputStream in = supplier.getInput()) {
        ByteStreams.copy(in, out);
      }
    }
  }

  public static void writeFloatMetric(
      OutputSupplier<? extends OutputStream> outSupplier, String name, CompressedFloatsSupplierSerializer column
  ) throws IOException
  {
    ByteStreams.write(version, outSupplier);
    serializerUtils.writeString(outSupplier, name);
    serializerUtils.writeString(outSupplier, "float");
    column.closeAndConsolidate(outSupplier);
  }

  public static void writeDoubleMetric(
      OutputSupplier<? extends OutputStream> outSupplier, String name, CompressedDoublesSupplierSerializer column
  ) throws IOException
  {
    ByteStreams.write(version, outSupplier);
    serializerUtils.writeString(outSupplier, name);
    serializerUtils.writeString(outSupplier, "double");
    column.closeAndConsolidate(outSupplier);
  }

  public static void writeLongMetric(
      OutputSupplier<? extends OutputStream> outSupplier, String name, CompressedLongsSupplierSerializer column
  ) throws IOException
  {
    ByteStreams.write(version, outSupplier);
    serializerUtils.writeString(outSupplier, name);
    serializerUtils.writeString(outSupplier, "long");
    column.closeAndConsolidate(outSupplier);
  }

  public static void writeToChannel(MetricHolder holder, WritableByteChannel out) throws IOException
  {
    out.write(ByteBuffer.wrap(version));
    serializerUtils.writeString(out, holder.name);
    serializerUtils.writeString(out, holder.typeName);

    switch (holder.type) {
      case FLOAT:
        holder.floatType.writeToChannel(out);
        break;
      case DOUBLE:
        holder.doubleType.writeToChannel(out);
        break;
      case LONG:
        holder.longType.writeToChannel(out);
        break;
      case COMPLEX:
        if (holder.complexType instanceof GenericIndexed) {
          ((GenericIndexed) holder.complexType).writeToChannel(out);
        } else {
          throw new IAE("Cannot serialize out MetricHolder for complex type that is not a GenericIndexed");
        }
        break;
    }
  }

  public static MetricHolder fromByteBuffer(ByteBuffer buf) throws IOException
  {
    return fromByteBuffer(buf, null);
  }

  public static MetricHolder fromByteBuffer(ByteBuffer buf, ObjectStrategy strategy) throws IOException
  {
    final byte ver = buf.get();
    if (version[0] != ver) {
      throw new ISE("Unknown version[%s] of MetricHolder", ver);
    }

    final String metricName = serializerUtils.readString(buf);
    final String typeName = serializerUtils.readString(buf);
    MetricHolder holder = new MetricHolder(metricName, typeName);

    switch (holder.type) {
      case LONG:
        holder.longType = CompressedLongsIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder());
        break;
      case FLOAT:
        holder.floatType = CompressedFloatsIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder());
        break;
      case DOUBLE:
        holder.doubleType = CompressedDoublesIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder());
        break;
      case COMPLEX:
        final ComplexMetricSerde serdeForType = ComplexMetrics.getSerdeForType(holder.getTypeName());

        if (serdeForType == null) {
          throw new ISE("Unknown type[%s], cannot load.", holder.getTypeName());
        }

        holder.complexType = GenericIndexed.read(buf, serdeForType.getObjectStrategy());
        break;
    }

    return holder;
  }

  private final String name;
  private final String typeName;
  private final ValueType type;

  CompressedLongsIndexedSupplier longType = null;
  CompressedFloatsIndexedSupplier floatType = null;
  CompressedDoublesIndexedSupplier doubleType = null;
  Indexed complexType = null;

  private MetricHolder(
      String name,
      String typeName
  )
  {
    this.name = name;
    this.typeName = typeName;
    this.type = ValueType.of(typeName);
  }

  public String getName()
  {
    return name;
  }

  public String getTypeName()
  {
    return typeName;
  }

  public ValueType getType()
  {
    return type;
  }

  public IndexedLongs getLongType()
  {
    assertType(ValueType.LONG);
    return longType.get();
  }

  public IndexedFloats getFloatType()
  {
    assertType(ValueType.FLOAT);
    return floatType.get();
  }

  public IndexedDoubles getDoubleType()
  {
    assertType(ValueType.DOUBLE);
    return doubleType.get();
  }

  public Indexed getComplexType()
  {
    assertType(ValueType.COMPLEX);
    return complexType;
  }

  private void assertType(ValueType type)
  {
    if (this.type != type) {
      throw new IAE("type[%s] cannot be cast to [%s]", typeName, type);
    }
  }
}
