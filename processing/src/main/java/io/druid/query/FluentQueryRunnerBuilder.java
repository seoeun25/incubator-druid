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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;

import java.util.concurrent.atomic.AtomicLong;

public class FluentQueryRunnerBuilder<T>
{
  private final QueryToolChest<T, Query<T>> toolChest;
  private final QueryRunner<T> baseRunner;

  public static <T> FluentQueryRunnerBuilder<T> create(QueryToolChest<T, Query<T>> toolChest, QueryRunner<T> baseRunner)
  {
    return new FluentQueryRunnerBuilder<T>(toolChest, baseRunner);
  }

  private FluentQueryRunnerBuilder(QueryToolChest<T, Query<T>> toolChest, QueryRunner<T> baseRunner)
  {
    this.toolChest = toolChest;
    this.baseRunner = baseRunner;
  }

  public QueryRunner<T> build()
  {
    return baseRunner;
  }

  public FluentQueryRunnerBuilder<T> from(QueryRunner<T> runner)
  {
    return new FluentQueryRunnerBuilder<T>(toolChest, runner);
  }

  public FluentQueryRunnerBuilder<T> applyRetry(RetryQueryRunnerConfig config, ObjectMapper jsonMapper)
  {
    return from(new RetryQueryRunner<T>(baseRunner, config, jsonMapper));
  }

  public FluentQueryRunnerBuilder<T> applyPreMergeDecoration()
  {
    return from(new UnionQueryRunner<T>(toolChest.preMergeQueryDecoration(baseRunner)));
  }

  public FluentQueryRunnerBuilder<T> applyMergeResults()
  {
    return from(toolChest.mergeResults(baseRunner));
  }

  public FluentQueryRunnerBuilder<T> applyPostMergeDecoration()
  {
    return from(toolChest.postMergeQueryDecoration(baseRunner));
  }

  public FluentQueryRunnerBuilder<T> applyFinalizeResults()
  {
    return from(toolChest.finalizeResults(baseRunner));
  }

  public FluentQueryRunnerBuilder<T> applyFinalQueryDecoration()
  {
    return from(toolChest.finalQueryDecoration(baseRunner));
  }

  public FluentQueryRunnerBuilder<T> applyPostProcessingOperator(ObjectMapper mapper)
  {
    return from(PostProcessingOperators.wrap(baseRunner, mapper));
  }

  public FluentQueryRunnerBuilder<T> emitCPUTimeMetric(ServiceEmitter emitter)
  {
    return from(
        CPUTimeMetricQueryRunner.safeBuild(
            baseRunner,
            toolChest.makeMetricBuilder(),
            emitter,
            new AtomicLong(0L),
            true
        )
    );
  }
}
