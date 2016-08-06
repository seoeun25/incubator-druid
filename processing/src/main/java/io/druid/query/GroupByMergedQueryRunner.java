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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.segment.incremental.IncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class GroupByMergedQueryRunner<T> implements QueryRunner<T>
{
  private static final String CTX_KEY_IS_SINGLE_THREADED = "groupByIsSingleThreaded";

  private static final Logger log = new Logger(GroupByMergedQueryRunner.class);
  private final Iterable<QueryRunner<T>> queryables;
  private final ListeningExecutorService exec;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final QueryWatcher queryWatcher;
  private final StupidPool<ByteBuffer> bufferPool;

  public GroupByMergedQueryRunner(
      ExecutorService exec,
      Supplier<GroupByQueryConfig> configSupplier,
      QueryWatcher queryWatcher,
      StupidPool<ByteBuffer> bufferPool,
      Iterable<QueryRunner<T>> queryables
  )
  {
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.configSupplier = configSupplier;
    this.bufferPool = bufferPool;
  }

  @Override
  public Sequence<T> run(final Query<T> queryParam, final Map<String, Object> responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;
    if (Iterables.size(queryables) == 1) {
      return Iterables.getOnlyElement(queryables).run(queryParam, responseContext);
    }

    final boolean isSingleThreaded = query.getContextValue(
        CTX_KEY_IS_SINGLE_THREADED,
        configSupplier.get().isSingleThreaded()
    );

    final Pair<IncrementalIndex, Accumulator<IncrementalIndex, T>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        configSupplier.get(),
        bufferPool
    );
    final Pair<Queue, Accumulator<Queue, T>> bySegmentAccumulatorPair = GroupByQueryHelper.createBySegmentAccumulatorPair();
    final boolean bySegment = BaseQuery.getContextBySegment(query, false);
    final int priority = BaseQuery.getContextPriority(query, 0);

    ListenableFuture<List<Void>> futures = Futures.allAsList(
        Lists.newArrayList(
            Iterables.transform(
                queryables,
                new Function<QueryRunner<T>, ListenableFuture<Void>>()
                {
                  @Override
                  public ListenableFuture<Void> apply(final QueryRunner<T> input)
                  {
                    if (input == null) {
                      throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                    }

                    ListenableFuture<Void> future = exec.submit(
                        new AbstractPrioritizedCallable<Void>(priority)
                        {
                          @Override
                          public Void call() throws Exception
                          {
                            try {
                              if (bySegment) {
                                input.run(queryParam, responseContext)
                                     .accumulate(bySegmentAccumulatorPair.lhs, bySegmentAccumulatorPair.rhs);
                              } else {
                                input.run(queryParam, responseContext)
                                     .accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
                              }

                              return null;
                            }
                            catch (QueryInterruptedException e) {
                              throw Throwables.propagate(e);
                            }
                            catch (Exception e) {
                              log.error(e, "Exception with one of the sequences!");
                              throw Throwables.propagate(e);
                            }
                          }
                        }
                    );

                    if (isSingleThreaded) {
                      waitForFutureCompletion(query, future, indexAccumulatorPair.lhs);
                    }

                    return future;
                  }
                }
            )
        )
    );

    if (!isSingleThreaded) {
      waitForFutureCompletion(query, futures, indexAccumulatorPair.lhs);
    }

    if (bySegment) {
      return Sequences.simple(bySegmentAccumulatorPair.lhs);
    }

    return new ResourceClosingSequence<T>(
        Sequences.simple(
            Iterables.transform(
                indexAccumulatorPair.lhs.iterableWithPostAggregations(null, query.isDescending()),
                new Function<Row, T>()
                {
                  @Override
                  public T apply(Row input)
                  {
                    return (T) input;
                  }
                }
            )
        ), indexAccumulatorPair.lhs
    );
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<?> future,
      IncrementalIndex<?> closeOnFailure
  )
  {
    try {
      queryWatcher.registerQuery(query, future);
      final Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT, (Number) null);
      if (timeout == null) {
        future.get();
      } else {
        future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      future.cancel(true);
      closeOnFailure.close();
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      closeOnFailure.close();
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      closeOnFailure.close();
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      closeOnFailure.close();
      throw Throwables.propagate(e.getCause());
    }
  }
}
