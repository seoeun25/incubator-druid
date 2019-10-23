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

package io.druid.data.input;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.common.Yielders;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BulkRowSequenceTest
{
  @Test
  public void test() throws IOException
  {
    Sequence<Row> rows = Sequences.simple(Arrays.<Row>asList(
        cr(0), cr(1), cr(2), cr(3), cr(4)
    ));
    BulkRowSequence bulk = new BulkRowSequence(rows, Arrays.asList(ValueDesc.LONG, ValueDesc.STRING), 2);

    final List<long[]> longs = Sequences.toList(Sequences.map(
        bulk, new Function<Row, long[]>()
        {
          @Override
          public long[] apply(Row input)
          {
            List<Long> timestamps = Lists.newArrayList();
            for (Row row : Sequences.toList(((BulkRow) input).decompose())) {
              timestamps.add((Long)((CompactRow) row).getValues()[0]);
            }
            return Longs.toArray(timestamps);
          }
        }
    ));
    Assert.assertArrayEquals(new long[]{0, 1}, longs.get(0));
    Assert.assertArrayEquals(new long[]{2, 3}, longs.get(1));
    Assert.assertArrayEquals(new long[]{4}, longs.get(2));

    final List<String[]> strings = Sequences.toList(Sequences.map(
        bulk, new Function<Row, String[]>()
        {
          @Override
          public String[] apply(Row input)
          {
            List<String> strings = Lists.newArrayList();
            for (Row row : Sequences.toList(((BulkRow) input).decompose())) {
              strings.add((String)((CompactRow) row).getValues()[1]);
            }
            return strings.toArray(new String[0]);
          }
        }
    ));
    Assert.assertArrayEquals(new String[]{"0", "1"}, strings.get(0));
    Assert.assertArrayEquals(new String[]{"2", "3"}, strings.get(1));
    Assert.assertArrayEquals(new String[]{"4"}, strings.get(2));

    Yielder<Row> yielder = bulk.toYielder(null, new Yielders.Yielding<Row>());
    List<Long> timestamps = Lists.newArrayList();
    for (Row row : Sequences.toList(((BulkRow) yielder.get()).decompose())) {
      timestamps.add((Long)((CompactRow) row).getValues()[0]);
    }
    Assert.assertArrayEquals(new long[]{0, 1}, Longs.toArray(timestamps));
    yielder = yielder.next(null);
    timestamps.clear();
    for (Row row : Sequences.toList(((BulkRow) yielder.get()).decompose())) {
      timestamps.add((Long)((CompactRow) row).getValues()[0]);
    }
    Assert.assertArrayEquals(new long[]{2, 3}, Longs.toArray(timestamps));
    yielder = yielder.next(null);
    timestamps.clear();
    for (Row row : Sequences.toList(((BulkRow) yielder.get()).decompose())) {
      timestamps.add((Long)((CompactRow) row).getValues()[0]);
    }
    Assert.assertTrue(yielder.isDone());

    DefaultObjectMapper mapper = new DefaultObjectMapper(new SmileFactory());
    byte[] s = mapper.writeValueAsBytes(bulk);
    List<Row> deserialized = mapper.readValue(s, new TypeReference<List<Row>>() {});
    Assert.assertEquals(
        Lists.newArrayList(cr(0), cr(1)), Sequences.toList(((BulkRow) deserialized.get(0)).decompose())
    );
    Assert.assertEquals(
        Lists.newArrayList(cr(2), cr(3)), Sequences.toList(((BulkRow) deserialized.get(1)).decompose())
    );
    Assert.assertEquals(
        Lists.newArrayList(cr(4)), Sequences.toList(((BulkRow) deserialized.get(2)).decompose())
    );
  }

  private CompactRow cr(long x)
  {
    return new CompactRow(new Object[]{x, String.valueOf(x)});
  }
}