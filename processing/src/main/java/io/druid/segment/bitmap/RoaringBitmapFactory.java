/*
 * Copyright 2011 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.bitmap;

import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableBitSetBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringArray;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;

// default implementation of union/intersection makes a lot of copy, which is not necessary for our use case
// simply using bitset and return back to roaring bitmap out-performs in the most of real use-cases (it's worse in single threaded micro test)
public final class RoaringBitmapFactory extends com.metamx.collections.bitmap.RoaringBitmapFactory
{
  public RoaringBitmapFactory()
  {
    super();
  }

  public RoaringBitmapFactory(boolean compressRunOnSerialization)
  {
    super(compressRunOnSerialization);
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return new WrappedImmutableRoaringBitmap(((WrappedImmutableRoaringBitmap) super.makeEmptyImmutableBitmap()).getBitmap())
    {
      @Override
      public boolean get(int value)
      {
        return false;
      }
    };
  }

  private static final short SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
  private static final short SERIAL_COOKIE = 12347;

  private static final short SMALL_COOKIE = 12345;

  private static final int CARDINALITY_THRESHOLD = 8;
  private static final int EXPECTED_MAX_LENGTH = 32;

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    return new WrappedImmutableRoaringBitmap(((WrappedImmutableRoaringBitmap) super.makeImmutableBitmap(mutableBitmap)).getBitmap())
    {
      @Override
      public byte[] toBytes()
      {
        final ImmutableRoaringBitmap bitmap = getBitmap();
        final int cardinality = bitmap.getCardinality();
        if (cardinality < CARDINALITY_THRESHOLD) {
          final BytesOutputStream out = new BytesOutputStream(EXPECTED_MAX_LENGTH);
          out.writeInt(Integer.reverseBytes(SMALL_COOKIE | cardinality << 16));
          final IntIterator iterator = bitmap.getIntIterator();
          int prev = 0;
          while (iterator.hasNext()) {
            final int value = iterator.next();
            out.writeUnsignedVarInt(value - prev);    // write delta
            prev = value;
          }
          return out.toByteArray();
        }
        return super.toBytes();
      }
    };
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> bitmaps)
  {
    final Iterator<ImmutableBitmap> iterator = bitmaps.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = iterator.next();
    if (!iterator.hasNext()) {
      return first;
    }
    final BitSet bitSet = copyTo(first, new BitSet());
    while (iterator.hasNext()) {
      copyTo(iterator.next(), bitSet);
    }
    return finalize(bitSet);
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> bitmaps)
  {
    final Iterator<ImmutableBitmap> iterator = bitmaps.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap first = iterator.next();
    if (!iterator.hasNext()) {
      return first;
    }
    final BitSet bitSet = copyTo(first, new BitSet());
    while (iterator.hasNext() && !bitSet.isEmpty()) {
      final ImmutableBitmap bitmap = iterator.next();
      if (bitmap.isEmpty()) {
        return makeEmptyImmutableBitmap();
      }
      int last = -1;
      int next = bitSet.nextSetBit(0);
      final IntIterator values = bitmap.iterator();
      while (values.hasNext() && next >= 0) {
        final int x = values.next();
        if (x > next) {
          bitSet.clear(next, x);
          next = bitSet.nextSetBit(x + 1);
        } else if (x == next) {
          next = bitSet.nextSetBit(x + 1);
        }
        last = x;
      }
      if (bitSet.get(last)) {
        last++;
      }
      if (last < bitSet.size()) {
        bitSet.clear(last, bitSet.size());
      }
    }
    return finalize(bitSet);
  }

  private BitSet copyTo(final ImmutableBitmap bitmap, final BitSet bitSet)
  {
    final IntIterator iterator = bitmap.iterator();
    while (iterator.hasNext()) {
      bitSet.set(iterator.next());
    }
    return bitSet;
  }

  private ImmutableBitmap finalize(final BitSet bitSet)
  {
    if (bitSet == null || bitSet.isEmpty()) {
      return makeEmptyImmutableBitmap();
    }
    return copyToBitmap(new WrappedImmutableBitSetBitmap(bitSet).iterator());
  }

  // should return -1 instead of NoSuchElementException
  public static ImmutableBitmap copyToBitmap(final IntIterator iterator)
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    final MutableRoaringArray roaringArray = mutable.getMappeableRoaringArray();

    short current_hb = 0;
    final IntArrayList values = new IntArrayList();
    for (int x = iterator.next(); x >= 0; x = iterator.next()) {
      final short hb = RoaringUtils.highbits(x);
      if (hb != current_hb && !values.isEmpty()) {
        RoaringUtils.addContainer(roaringArray, current_hb, values);
        values.clear();
      }
      current_hb = hb;
      values.add(x);
    }
    if (!values.isEmpty()) {
      RoaringUtils.addContainer(roaringArray, current_hb, values);
    }
    values.clear();
    return new WrappedImmutableRoaringBitmap(mutable);
  }

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer bbf)
  {
    final ByteBuffer buffer = bbf.order(ByteOrder.LITTLE_ENDIAN);
    final int cookie = buffer.getInt(buffer.position()) & 0xFFFF;
    if (cookie == SMALL_COOKIE) {
      final ByteBuffer readOnly = buffer.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN); // order is not propagated
      final int size = readOnly.getInt() >>> 16;
      if (size == 0) {
        return makeEmptyImmutableBitmap();
      }
      return copyToBitmap(new IntIterators.Abstract()
      {
        private final ByteBuffer bigEndian = readOnly.order(ByteOrder.BIG_ENDIAN);
        private int index = 0;
        private int prev = 0;

        @Override
        public boolean hasNext()
        {
          return index < size;
        }

        @Override
        public int next()
        {
          return index++ < size ? (prev += BytesInputStream.readUnsignedVarInt(bigEndian)) : -1;
        }
      });
    }
    return super.mapImmutableBitmap(bbf);
  }
}
