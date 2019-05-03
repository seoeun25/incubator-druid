package io.druid.indexer.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.druid.data.input.InputRow;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;

public class StreamInputFormat extends FileInputFormat
{
  private InputRowParser.Streaming<?> streaming;

  @Override
  protected final boolean isSplitable(JobContext context, Path filename)
  {
    return false;
  }

  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    if (streaming == null) {
      InputRowParser parser = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration()).getParser();
      Preconditions.checkArgument(parser instanceof InputRowParser.Streaming, "not streaming parser %s", parser);
      streaming = (InputRowParser.Streaming) parser;
    }
    return new RecordReader()
    {
      private FileSplit fileSplit;
      private FSDataInputStream inputStream;
      private Iterator<InputRow> iterator = Iterators.emptyIterator();

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
        fileSplit = (FileSplit) split;
        // open the file and seek to the start of the split
        final Path path = fileSplit.getPath();
        final FileSystem fs = path.getFileSystem(context.getConfiguration());
        inputStream = fs.open(path);
        iterator = Preconditions.checkNotNull(InputRowParsers.toStreamIterator(streaming, inputStream));
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        return iterator.hasNext();
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException
      {
        return null;
      }

      @Override
      public Object getCurrentValue() throws IOException, InterruptedException
      {
        return iterator.next();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        return inputStream == null ? 0f : ((float) inputStream.getPos() - fileSplit.getStart()) / fileSplit.getLength();
      }

      @Override
      public void close() throws IOException
      {
        if (inputStream != null) {
          inputStream.close();
        }
      }
    };
  }
}