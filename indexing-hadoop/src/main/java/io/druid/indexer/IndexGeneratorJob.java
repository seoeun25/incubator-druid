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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class IndexGeneratorJob implements HadoopDruidIndexerJob.IndexingStatsProvider
{
  private static final Logger log = new Logger(IndexGeneratorJob.class);

  public static List<DataSegment> getPublishedSegments(HadoopDruidIndexerConfig config)
  {
    final Configuration conf = JobHelper.injectSystemProperties(new Configuration());
    final ObjectMapper jsonMapper = HadoopDruidIndexerConfig.JSON_MAPPER;

    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();

    final Path descriptorInfoDir = config.makeDescriptorInfoDir();

    try {
      FileSystem fs = descriptorInfoDir.getFileSystem(conf);
      if (!fs.exists(descriptorInfoDir)) {
        log.warn(
            "Cannot find descriptor directory [%s].. possible when any index was not created (invalid input path?)",
            descriptorInfoDir
        );
      } else {
        FileStatus[] segments = fs.listStatus(descriptorInfoDir);
        log.info("Indexing job reported total %d segments created", segments.length);
        for (FileStatus status : segments) {
          final DataSegment segment = jsonMapper.readValue(fs.open(status.getPath()), DataSegment.class);
          publishedSegmentsBuilder.add(segment);
          log.info("Adding segment %s to the list of published segments", segment.getIdentifier());
        }
      }
    }
    catch (FileNotFoundException e) {
      log.error(
          "[%s] SegmentDescriptorInfo is not found usually when indexing process did not produce any segments meaning"
          + " either there was no input data to process or all the input events were discarded due to some error",
          e.getMessage()
      );
      Throwables.propagate(e);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return publishedSegmentsBuilder.build();
  }

  private final HadoopDruidIndexerConfig config;
  private IndexGeneratorStats jobStats;

  public IndexGeneratorJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
    this.jobStats = new IndexGeneratorStats();
  }

  protected void setReducerClass(final Job job)
  {
    job.setReducerClass(IndexGeneratorReducer.class);
  }

  public IndexGeneratorStats getJobStats()
  {
    return jobStats;
  }

  public boolean run()
  {
    log.info("Running IndexGeneratorJob.. %s %s", config.getDataSource(), config.getIntervals());
    try {
      Job job = Job.getInstance(
          new Configuration(),
          String.format("%s-index-generator-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.23");

      JobHelper.injectSystemProperties(job);
      config.addJobProperties(job);

      job.setMapperClass(IndexGeneratorMapper.class);
      job.setMapOutputValueClass(BytesWritable.class);

      SortableBytes.useSortableBytesAsMapOutputKey(job);

      int numReducers = Math.min(config.getMaxReducer(), Iterables.size(config.getAllBuckets().get()));
      if (numReducers == 0) {
        throw new RuntimeException("No buckets?? seems there is no data to index.");
      }

      if (config.getGranularitySpec().isAppending()) {
        throw new RuntimeException("Does not support segment appending in this mode.");
      }
      if (config.getSchema().getTuningConfig().getUseCombiner()) {
        // when settling is used, combiner should be turned off
        if (config.getSchema().getSettlingConfig() == null) {
          job.setCombinerClass(IndexGeneratorCombiner.class);
          job.setCombinerKeyGroupingComparatorClass(BytesWritable.Comparator.class);
        } else {
          log.error("Combiner is set but ignored as settling cannot use combiner");
        }
      }

      job.setNumReduceTasks(numReducers);
      job.setPartitionerClass(IndexGeneratorPartitioner.class);

      setReducerClass(job);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(IndexGeneratorOutputFormat.class);
      FileOutputFormat.setOutputPath(job, config.makeIntermediatePath());

      config.addInputPaths(job);

      config.intoConfiguration(job);

      JobHelper.setupClasspath(
          JobHelper.distributedClassPath(config.getWorkingPath()),
          JobHelper.distributedClassPath(config.makeIntermediatePath()),
          job
      );

      job.submit();
      log.info("Job %s submitted, status available at %s", job.getJobName(), job.getTrackingURL());

      boolean success = job.waitForCompletion(true);

      Counter invalidRowCount = job.getCounters()
                                   .findCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER);
      jobStats.setInvalidRowCount(invalidRowCount.getValue());

      return success;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static IncrementalIndex makeIncrementalIndex(
      Bucket theBucket,
      AggregatorFactory[] aggs,
      HadoopDruidIndexerConfig config,
      Iterable<String> oldDimOrder
  )
  {
    return makeIncrementalIndex(theBucket, aggs, config, oldDimOrder, -1);
  }

  private static IncrementalIndex makeIncrementalIndex(
      Bucket theBucket,
      AggregatorFactory[] aggs,
      HadoopDruidIndexerConfig config,
      Iterable<String> oldDimOrder,
      int hardLimit
  )
  {
    final HadoopTuningConfig tuningConfig = config.getSchema().getTuningConfig();
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(theBucket.time.getMillis())
        .withDimensionsSpec(config.getSchema().getDataSchema().getParser())
        .withQueryGranularity(config.getSchema().getDataSchema().getGranularitySpec().getQueryGranularity())
        .withMetrics(aggs)
        .withRollup(config.getSchema().getDataSchema().getGranularitySpec().isRollup())
        .build();

    final int boundary = hardLimit < 0 ? tuningConfig.getRowFlushBoundary() : hardLimit;
    OnheapIncrementalIndex newIndex = new OnheapIncrementalIndex(
        indexSchema,
        true,
        !tuningConfig.isIgnoreInvalidRows(),
        !tuningConfig.isAssumeTimeSorted(),
        boundary
    );

    if (oldDimOrder != null && !indexSchema.getDimensionsSpec().hasCustomDimensions()) {
      newIndex.loadDimensionIterable(oldDimOrder);
    }

    return newIndex;
  }

  public static class IndexGeneratorMapper extends HadoopDruidIndexerMapper<BytesWritable, BytesWritable>
  {
    private static final HashFunction hashFunction = Hashing.murmur3_128();

    private List<String> partitionDimensions;
    private InputRowSerde serde;

    private Counter keyLength;
    private Counter valLength;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);

      List<String> exportDimensions = config.extractForwardingColumns();

      partitionDimensions = Lists.newArrayList(exportDimensions);
      SettlingConfig settlingConfig = config.getSchema().getSettlingConfig();
      if (settlingConfig != null) {
        partitionDimensions.remove(settlingConfig.getParamNameColumn());
        partitionDimensions.remove(settlingConfig.getParamValueColumn());
      }
      serde = new InputRowSerde(config.getSchema().getDataSchema().getAggregators(), exportDimensions);

      keyLength = context.getCounter("navis", "mapper-key-length");
      valLength = context.getCounter("navis", "mapper-value-length");
    }

    @Override
    protected void innerMap(
        InputRow inputRow,
        Object value,
        Context context
    ) throws IOException, InterruptedException
    {
      // Group by bucket, sort by timestamp
      final Optional<Bucket> bucket = getConfig().getBucket(inputRow);

      if (!bucket.isPresent()) {
        throw new ISE("WTF?! No bucket found for row: %s", inputRow);
      }

      final long timestampFromEpoch = inputRow.getTimestampFromEpoch();
      final long truncatedTimestamp = granularitySpec.getQueryGranularity().truncate(timestampFromEpoch);
      final byte[] hashedDimensions = Rows.toGroupHash(
          hashFunction.newHasher(), truncatedTimestamp, inputRow, partitionDimensions
      ).asBytes();

      // type SegmentInputRow serves as a marker that these InputRow instances have already been combined
      // and they contain the columns as they show up in the segment after ingestion, not what you would see in raw
      // data
      byte[] serializedInputRow = serde.serialize(inputRow);

      BytesWritable key = new SortableBytes(
          bucket.get().toGroupKey(),
          // sort rows by truncated timestamp and hashed dimensions to help reduce spilling on the reducer side
          ByteBuffer.allocate(Longs.BYTES + hashedDimensions.length + Longs.BYTES)
                    .putLong(truncatedTimestamp)
                    .put(hashedDimensions)
                    .putLong(timestampFromEpoch)
                    .array()
      ).toBytesWritable();

      context.write(key, new BytesWritable(serializedInputRow));

      keyLength.increment(key.getLength());
      valLength.increment(serializedInputRow.length);
    }
  }

  public static class IndexGeneratorCombiner extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable>
  {
    private HadoopDruidIndexerConfig config;
    private List<String> dimensions;
    private AggregatorFactory[] aggregators;
    private AggregatorFactory[] combiningAggs;

    private InputRowSerde readSerde;
    private InputRowSerde writeSerde;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());

      dimensions = config.extractForwardingColumns();
      aggregators = config.getSchema().getDataSchema().getAggregators();
      combiningAggs = new AggregatorFactory[aggregators.length];
      for (int i = 0; i < aggregators.length; ++i) {
        combiningAggs[i] = aggregators[i].getCombiningFactory();
      }
      readSerde = new InputRowSerde(aggregators, dimensions);
      writeSerde = new InputRowSerde(combiningAggs, dimensions);
    }

    @Override
    protected void reduce(
        final BytesWritable key, Iterable<BytesWritable> values, final Context context
    ) throws IOException, InterruptedException
    {

      Iterator<BytesWritable> iter = values.iterator();
      BytesWritable first = iter.next();

      if (iter.hasNext()) {
        LinkedHashSet<String> dimOrder = Sets.newLinkedHashSet();
        SortableBytes keyBytes = SortableBytes.fromBytesWritable(key);
        Bucket bucket = Bucket.fromGroupKey(keyBytes.getGroupKey()).lhs;
        IncrementalIndex index = makeIncrementalIndex(bucket, combiningAggs, config, null);
        index.add(readSerde.deserialize(first.getBytes()));

        while (iter.hasNext()) {
          context.progress();
          InputRow value = readSerde.deserialize(iter.next().getBytes());

          if (!index.canAppendRow()) {
            dimOrder.addAll(index.getDimensionOrder());
            log.info("current index full due to [%s]. creating new index.", index.getOutOfRowsReason());
            flushIndexToContextAndClose(key, index, context);
            index = makeIncrementalIndex(bucket, combiningAggs, config, dimOrder);
          }

          index.add(value);
        }

        flushIndexToContextAndClose(key, index, context);
      } else {
        context.write(key, first);
      }
    }

    private void flushIndexToContextAndClose(BytesWritable key, IncrementalIndex index, Context context)
        throws IOException, InterruptedException
    {
      final List<String> dimensions = index.getDimensionNames();
      Iterator<Row> rows = index.iterator();
      while (rows.hasNext()) {
        context.progress();
        Row row = rows.next();
        InputRow inputRow = getInputRowFromRow(row, dimensions);
        context.write(
            key,
            new BytesWritable(writeSerde.serialize(inputRow))
        );
      }
      index.close();
    }

    private InputRow getInputRowFromRow(final Row row, final List<String> dimensions)
    {
      return new InputRow()
      {
        @Override
        public List<String> getDimensions()
        {
          return dimensions;
        }

        @Override
        public long getTimestampFromEpoch()
        {
          return row.getTimestampFromEpoch();
        }

        @Override
        public DateTime getTimestamp()
        {
          return row.getTimestamp();
        }

        @Override
        public List<String> getDimension(String dimension)
        {
          return row.getDimension(dimension);
        }

        @Override
        public Object getRaw(String dimension)
        {
          return row.getRaw(dimension);
        }

        @Override
        public float getFloatMetric(String metric)
        {
          return row.getFloatMetric(metric);
        }

        @Override
        public double getDoubleMetric(String metric)
        {
          return row.getDoubleMetric(metric);
        }

        @Override
        public long getLongMetric(String metric)
        {
          return row.getLongMetric(metric);
        }

        @Override
        public Collection<String> getColumns()
        {
          return row.getColumns();
        }

        @Override
        public int compareTo(Row o)
        {
          return row.compareTo(o);
        }
      };
    }
  }

  public static class IndexGeneratorPartitioner extends Partitioner<BytesWritable, Writable> implements Configurable
  {
    private Configuration config;

    @Override
    public int getPartition(BytesWritable bytesWritable, Writable value, int numPartitions)
    {
      final ByteBuffer bytes = ByteBuffer.wrap(bytesWritable.getBytes());
      bytes.position(4); // Skip length added by SortableBytes
      int shardNum = bytes.getInt();
      if (config.get("mapred.job.tracker").equals("local")) {
        return shardNum % numPartitions;
      } else {
        if (shardNum >= numPartitions) {
          throw new ISE("Not enough partitions, shard[%,d] >= numPartitions[%,d]", shardNum, numPartitions);
        }
        return shardNum;

      }
    }

    @Override
    public Configuration getConf()
    {
      return config;
    }

    @Override
    public void setConf(Configuration config)
    {
      this.config = config;
    }
  }

  public static class IndexGeneratorReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, Text>
  {
    protected HadoopDruidIndexerConfig config;
    protected SettlingConfig settlingConfig;
    private String nameField;
    private String valueField;
    private SettlingConfig.Settler settler;
    private List<String> metricNames = Lists.newArrayList();
    private AggregatorFactory[] aggregators;
    private AggregatorFactory[] rangedAggs;

    private InputRowSerde serde;
    private Counter flushedIndex;
    private Counter groupCount;

    protected ProgressIndicator makeProgressIndicator(final Context context)
    {
      return new BaseProgressIndicator()
      {
        @Override
        public void progress()
        {
          super.progress();
          context.progress();
        }
      };
    }

    private File persist(
        final IncrementalIndex index,
        final Interval interval,
        final File file,
        final ProgressIndicator progressIndicator
    ) throws IOException
    {
      flushedIndex.increment(1);
      log.info(
          "flushing index.. %,d rows with estimated size %,d bytes (%s)",
          index.size(), index.estimatedOccupation(), interval
      );

      if (config.isBuildV9Directly()) {
        return HadoopDruidIndexerConfig.INDEX_MERGER_V9.persist(
            index, interval, file, config.getIndexSpec(), progressIndicator
        );
      } else {
        return HadoopDruidIndexerConfig.INDEX_MERGER.persist(
            index, interval, file, config.getIndexSpec(), progressIndicator
        );
      }
    }

    protected File mergeQueryableIndex(
        final List<QueryableIndex> indexes,
        final AggregatorFactory[] aggs,
        final File file,
        ProgressIndicator progressIndicator
    ) throws IOException
    {
      boolean rollup = config.getSchema().getDataSchema().getGranularitySpec().isRollup();
      if (config.isBuildV9Directly()) {
        return HadoopDruidIndexerConfig.INDEX_MERGER_V9.mergeQueryableIndexAndClose(
            indexes, rollup, aggs, file, config.getIndexSpec(), progressIndicator
        );
      } else {
        return HadoopDruidIndexerConfig.INDEX_MERGER.mergeQueryableIndexAndClose(
            indexes, rollup, aggs, file, config.getIndexSpec(), progressIndicator
        );
      }
    }

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
      aggregators = config.getSchema().getDataSchema().getAggregators();
      serde = new InputRowSerde(aggregators, config.extractForwardingColumns(), config.extractFinalDimensions());

      rangedAggs = new AggregatorFactory[aggregators.length];
      for (int i = 0; i < aggregators.length; ++i) {
        metricNames.add(aggregators[i].getName());
        rangedAggs[i] = aggregators[i];
      }

      settlingConfig = config.getSchema().getSettlingConfig();
      if (settlingConfig != null) {
        settler = settlingConfig.setUp(aggregators);
        nameField = settlingConfig.getParamNameColumn();
        valueField = settlingConfig.getParamValueColumn();
      }
      flushedIndex = context.getCounter("navis", "index-flush-count");
      groupCount = context.getCounter("navis", "group-count");
    }

    @Override
    protected void reduce(
        BytesWritable key, Iterable<BytesWritable> values, final Context context
    ) throws IOException, InterruptedException
    {
      SortableBytes keyBytes = SortableBytes.fromBytesWritable(key);
      Bucket bucket = Bucket.fromGroupKey(keyBytes.getGroupKey()).lhs;

      final Interval interval = config.getGranularitySpec().bucketInterval(bucket.time).get();
      final HadoopTuningConfig tuningConfig = config.getSchema().getTuningConfig();
      final int limit = tuningConfig.getRowFlushBoundary();
      final long maxOccupation = tuningConfig.getMaxOccupationInMemory();
      final int occupationCheckInterval = 2000;

      ListeningExecutorService persistExecutor = null;
      List<ListenableFuture<?>> persistFutures = Lists.newArrayList();
      IncrementalIndex index = makeIncrementalIndex(
          bucket,
          rangedAggs,
          config,
          null,
          limit << 1
      );
      try {
        File baseFlushFile = File.createTempFile("base", "flush");
        baseFlushFile.delete();
        baseFlushFile.mkdirs();

        Set<File> toMerge = Sets.newTreeSet();
        int indexCount = 0;
        int lineCount = 0;
        int runningTotalLineCount = 0;
        long startTime = System.currentTimeMillis();

        Set<String> allDimensionNames = Sets.newLinkedHashSet();
        final ProgressIndicator progressIndicator = makeProgressIndicator(context);
        int numBackgroundPersistThreads = tuningConfig.getNumBackgroundPersistThreads();
        if (numBackgroundPersistThreads > 0) {
          final BlockingQueue<Runnable> queue = new SynchronousQueue<>();
          ExecutorService executorService = new ThreadPoolExecutor(
              numBackgroundPersistThreads,
              numBackgroundPersistThreads,
              0L,
              TimeUnit.MILLISECONDS,
              queue,
              Execs.makeThreadFactory("IndexGeneratorJob_persist_%d"),
              new RejectedExecutionHandler()
              {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
                {
                  try {
                    executor.getQueue().put(r);
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RejectedExecutionException("Got Interrupted while adding to the Queue");
                  }
                }
              }
          );
          persistExecutor = MoreExecutors.listeningDecorator(executorService);
        } else {
          persistExecutor = MoreExecutors.sameThreadExecutor();
        }

        int lengthSkipLastTS = key.getLength() - Longs.BYTES;
        byte[] prev = null;

        int numRows = 0;
        int nextLogging = 1000;
        AggregatorFactory[][] settlingApplied = null;
        for (final BytesWritable bw : values) {
          context.progress();

          final InputRow inputRow = index.formatRow(serde.deserialize(bw.getBytes()));

          byte[] bytes = key.getBytes();
          boolean flush = !index.canAppendRow();
          if (prev == null || WritableComparator.compareBytes(prev, 0, prev.length, bytes, 0, prev.length) != 0) {
            prev = Arrays.copyOfRange(bytes, 0, lengthSkipLastTS);
            if (settler != null) {
              settlingApplied = settler.applySettling(inputRow);
            }
            flush |= numRows >= limit;
            groupCount.increment(1);
            if (groupCount.getValue() % occupationCheckInterval == 0) {
              long estimation = index.estimatedOccupation();
              log.info("... %,d rows in index with estimated size %,d bytes", index.size(), estimation);
              if (!flush && maxOccupation > 0 && estimation >= maxOccupation) {
                log.info("flushing index because estimated size is bigger than %,d", maxOccupation);
                flush = true;
              }
            }
          }
          if (flush) {
            allDimensionNames.addAll(index.getDimensionOrder());

            log.info(index.getOutOfRowsReason());
            log.info(
                "%,d lines to %,d rows in %,d millis",
                lineCount - runningTotalLineCount,
                numRows,
                System.currentTimeMillis() - startTime
            );
            runningTotalLineCount = lineCount;

            final File file = new File(baseFlushFile, String.format("index%,05d", indexCount));
            toMerge.add(file);

            context.progress();
            final IncrementalIndex persistIndex = index;
            persistFutures.add(
                persistExecutor.submit(
                    new ThreadRenamingRunnable(String.format("%s-persist", file.getName()))
                    {
                      @Override
                      public void doRun()
                      {
                        try {
                          persist(persistIndex, interval, file, progressIndicator);
                        }
                        catch (Exception e) {
                          log.error(e, "persist index error");
                          throw Throwables.propagate(e);
                        }
                        finally {
                          // close this index
                          persistIndex.close();
                        }
                      }
                    }
                )
            );

            index = makeIncrementalIndex(
                bucket,
                rangedAggs,
                config,
                allDimensionNames,
                limit << 1
            );
            startTime = System.currentTimeMillis();
            ++indexCount;
          }
          numRows = add(index, inputRow, settlingApplied);
          if (++lineCount % nextLogging == 0) {
            log.info("processing %,d lines..", lineCount);
            nextLogging = Math.min(nextLogging * 10, 1000000);
          };
        }

        allDimensionNames.addAll(index.getDimensionOrder());

        log.info("%,d lines completed.", lineCount);

        List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(indexCount);
        final File mergedBase;

        if (toMerge.size() == 0) {
          if (index.isEmpty()) {
            throw new IAE("If you try to persist empty indexes you are going to have a bad time");
          }

          mergedBase = new File(baseFlushFile, "merged");
          persist(index, interval, mergedBase, progressIndicator);
        } else {
          if (!index.isEmpty()) {
            final File finalFile = new File(baseFlushFile, "final");
            persist(index, interval, finalFile, progressIndicator);
            toMerge.add(finalFile);
          }

          Futures.allAsList(persistFutures).get(1, TimeUnit.HOURS);
          persistExecutor.shutdown();

          for (File file : toMerge) {
            indexes.add(HadoopDruidIndexerConfig.INDEX_IO.loadIndex(file));
          }
          mergedBase = mergeQueryableIndex(
              indexes, aggregators, new File(baseFlushFile, "merged"), progressIndicator
          );
        }
        final FileSystem outputFS = new Path(config.getSchema().getIOConfig().getSegmentOutputPath())
            .getFileSystem(context.getConfiguration());
        final DataSegment segmentTemplate = new DataSegment(
            config.getDataSource(),
            interval,
            tuningConfig.getVersion(),
            null,
            ImmutableList.copyOf(allDimensionNames),
            metricNames,
            config.getShardSpec(bucket).getActualSpec(),
            -1,
            -1
        );
        final DataSegment segment = JobHelper.serializeOutIndex(
            segmentTemplate,
            context.getConfiguration(),
            context,
            context.getTaskAttemptID(),
            mergedBase,
            JobHelper.makeSegmentOutputPath(
                new Path(config.getSchema().getIOConfig().getSegmentOutputPath()),
                outputFS,
                segmentTemplate
            )
        );

        Path descriptorPath = config.makeDescriptorInfoPath(segment);
        descriptorPath = JobHelper.prependFSIfNullScheme(
            FileSystem.get(
                descriptorPath.toUri(),
                context.getConfiguration()
            ), descriptorPath
        );

        log.info("Writing descriptor to path[%s]", descriptorPath);
        JobHelper.writeSegmentDescriptor(
            config.makeDescriptorInfoDir().getFileSystem(context.getConfiguration()),
            segment,
            descriptorPath,
            context
        );
        for (File file : toMerge) {
          FileUtils.deleteDirectory(file);
        }
      }
      catch (ExecutionException e) {
        throw Throwables.propagate(e);
      }
      catch (TimeoutException e) {
        throw Throwables.propagate(e);
      }
      finally {
        index.close();
        if (persistExecutor != null) {
          persistExecutor.shutdownNow();
        }
      }
    }

    private int add(IncrementalIndex index, InputRow row, AggregatorFactory[][] settlingApplied)
        throws IndexSizeExceededException
    {
      if (settlingApplied != null) {
        int ret = 0;
        MapBasedInputRow mapBasedInputRow = (MapBasedInputRow) row;
        Map<String, Object> event = mapBasedInputRow.getEvent();

        Object rawNameField = row.getRaw(nameField);
        Object rawValueField = row.getRaw(valueField);

        final List names = rawNameField instanceof List ? (List) rawNameField : Lists.newArrayList(rawNameField);
        final List values = rawValueField instanceof List ? (List) rawValueField : Lists.newArrayList(rawValueField);
        for (int i = 0; i < settlingApplied.length; i++) {
          Object value = toNumeric(values.get(i));
          if (value != null) {
            event.put(nameField, names.get(i));
            event.put(valueField, value);
            System.arraycopy(settlingApplied[i], 0, rangedAggs, 0, rangedAggs.length);
            ret = index.add(row);
          }
        }
        return ret;
      } else {
        return index.add(row);
      }
    }
  }

  private static Object toNumeric(Object value)
  {
    if (value == null || value instanceof Number) {
      return value;
    }

    if (value.equals("NaN")) {
      return Double.valueOf(0);
    }

    final String stringVal = String.valueOf(value);
    if (stringVal.isEmpty()) {
      return null;
    }
    int i = 0;
    char first = stringVal.charAt(i);
    if (!Character.isDigit(first)) {
      if (first != '+' && first != '-') {
        return null;
      }
      i++;
    }
    boolean metDot = false;
    boolean metExp = false;
    for (; i < stringVal.length(); i++) {
      char aChar = stringVal.charAt(i);
      if (!Character.isDigit(aChar)) {
        if (!metDot && aChar == '.') {
          metDot = true;
          continue;
        }
        if (!metExp && aChar == 'E') {
          metExp = true;
          if (i < stringVal.length() - 1 && stringVal.charAt(i + 1) == '-') {
            i++;
          }
          if (i == stringVal.length() - 1) {
            return null;
          }
          continue;
        }
        return null;
      }
    }
    if (metDot || metExp) {
      return Double.valueOf(stringVal);
    }
    return Long.valueOf(stringVal);
  }

  public static class IndexGeneratorOutputFormat extends TextOutputFormat
  {
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException
    {
      Path outDir = getOutputPath(job);
      if (outDir == null) {
        throw new InvalidJobConfException("Output directory not set.");
      }
    }
  }

  public static class IndexGeneratorStats
  {
    private long invalidRowCount = 0;

    public long getInvalidRowCount()
    {
      return invalidRowCount;
    }

    public void setInvalidRowCount(long invalidRowCount)
    {
      this.invalidRowCount = invalidRowCount;
    }
  }
}
