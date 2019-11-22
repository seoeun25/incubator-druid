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

import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.common.guava.Sequence;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FluentQueryRunnerBuilder<T>
{
  private static final Logger log = new Logger(FluentQueryRunnerBuilder.class);

  final QueryToolChest<T, Query<T>> toolChest;

  public FluentQueryRunner create(QueryRunner<T> baseRunner)
  {
    log.info("----az create");
    return new FluentQueryRunner(baseRunner);
  }

  public FluentQueryRunnerBuilder(QueryToolChest<T, Query<T>> toolChest)
  {
    this.toolChest = toolChest;
  }

  public class FluentQueryRunner implements QueryRunner<T>
  {
    private QueryRunner<T> baseRunner;

    public FluentQueryRunner(QueryRunner<T> runner)
    {
      this.baseRunner = runner;
      log.info("----az new FluentQueryRunner. baseRunner = %s", baseRunner.getClass().getSimpleName());
    }

    @Override
    public Sequence<T> run(
        QueryPlus<T> queryPlus, Map<String, Object> responseContext
    )
    {
      log.info("----az QueryRunner.run. baseRunner = %s", baseRunner.getClass().getSimpleName());
      return baseRunner.run(queryPlus, responseContext);
    }

    public FluentQueryRunner from(QueryRunner<T> runner)
    {
      return new FluentQueryRunner(runner);
    }

    public FluentQueryRunner applyPostMergeDecoration()
    {
      return from(
          new FinalizeResultsQueryRunner<T>(
              toolChest.postMergeQueryDecoration(
                  baseRunner
              ),
              toolChest
          )
      );
    }

    public FluentQueryRunner applyPreMergeDecoration()
    {
      return from(
          new UnionQueryRunner<T>(
              toolChest.preMergeQueryDecoration(
                  baseRunner
              )
          )
      );
    }

    public FluentQueryRunner emitCPUTimeMetric(ServiceEmitter emitter)
    {
      return from(
          CPUTimeMetricQueryRunner.safeBuild(
              baseRunner,
              toolChest,
              emitter,
              new AtomicLong(0L),
              true
          )
      );
    }

    public FluentQueryRunner postProcess(PostProcessingOperator<T> postProcessing)
    {
      log.info("----az postProcessing = %s, baseRunner = %s", postProcessing, baseRunner);
      return from(
          postProcessing != null ?
             postProcessing.postProcess(baseRunner) : baseRunner
      );
    }

    public FluentQueryRunner mergeResults()
    {
      log.info("---- az mergeResult");
      return from(
          toolChest.mergeResults(baseRunner)
      );
    }
  }
}
