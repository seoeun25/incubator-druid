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

package io.druid.query;

import com.google.common.base.Supplier;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.Segments;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final Supplier<RowResolver> resolver;
  private final QueryRunnerFactory<T, Query<T>> factory;
  private final ReferenceCountingSegment adapter;
  private final SegmentDescriptor descriptor;
  private final Future<Object> optimizer;

  public ReferenceCountingSegmentQueryRunner(
      Supplier<RowResolver> resolver,
      QueryRunnerFactory<T, Query<T>> factory,
      ReferenceCountingSegment adapter,
      SegmentDescriptor descriptor,
      Future<Object> optimizer
  )
  {
    this.resolver = resolver;
    this.factory = factory;
    this.adapter = adapter;
    this.descriptor = descriptor;
    this.optimizer = optimizer;
  }

  @Override
  public Sequence<T> run(final Query<T> query, Map<String, Object> responseContext)
  {
    final Closeable closeable = adapter.increment();
    if (closeable != null) {
      try {
        final Segment segment = Segments.attach(adapter, resolver);
        final Sequence<T> baseSequence = factory.createRunner(segment, optimizer).run(query, responseContext);

        return new ResourceClosingSequence<T>(baseSequence, closeable);
      }
      catch (RuntimeException e) {
        CloseQuietly.close(closeable);
        throw e;
      }
    } else {
      // Segment was closed before we had a chance to increment the reference count
      return new ReportTimelineMissingSegmentQueryRunner<T>(descriptor).run(query, responseContext);
    }
  }
}
