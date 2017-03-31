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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class HynixPathSpec implements PathSpec
{
  private static final int DEFAULT_SPLIT_SIZE = 0;

  public static final String PATH_SPECS = "hynix.input.path.specs";
  public static final String INPUT_FORMAT_OLD = "hynix.input.path.specs.format.old";
  public static final String INPUT_FORMAT_NEW = "hynix.input.path.specs.format.new";
  public static final String SPLIT_SIZE = "hynix.input.path.specs.split.size";

  public static final String FIND_RECURSIVE = FileInputFormat.INPUT_DIR_RECURSIVE;
  public static final String EXTRACT_PARTITION = "hynix.input.path.extract.partition";

  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)

  private final List<HynixPathSpecElement> elements;
  private final Class inputFormat;
  private final long splitSize;
  private final boolean findRecursive;
  private final boolean extractPartition;
  private final Map<String, Object> properties;

  @JsonCreator
  public HynixPathSpec(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("elements") List<HynixPathSpecElement> elements,
      @JsonProperty("inputFormat") Class inputFormat,
      @JsonProperty("splitSize") String splitSize,
      @JsonProperty("findRecursive") boolean findRecursive,
      @JsonProperty("extractPartition") boolean extractPartition,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    this.basePath = basePath;
    this.elements = Preconditions.checkNotNull(elements);
    this.inputFormat = inputFormat == null ? TextInputFormat.class : inputFormat;
    this.splitSize = StringUtils.parseKMGT(splitSize, DEFAULT_SPLIT_SIZE);
    this.findRecursive = findRecursive;
    this.extractPartition = extractPartition;
    this.properties = properties;
    Preconditions.checkArgument(!elements.isEmpty());
    Preconditions.checkArgument(basePath == null || new Path(basePath).isAbsolute());
    for (HynixPathSpecElement element : elements) {
      for (String path : HadoopGlobPathSplitter.splitGlob(element.getPaths())) {
        if (basePath == null) {
          Preconditions.checkArgument(new Path(path).isAbsolute());
        } else {
          Preconditions.checkArgument(!new Path(path).isAbsolute());
        }
      }
    }
  }

  protected HynixPathSpec(HynixPathSpec pathSpec)
  {
    this.basePath = pathSpec.basePath;
    this.elements = pathSpec.elements;
    this.inputFormat = pathSpec.inputFormat;
    this.splitSize = pathSpec.splitSize;
    this.findRecursive = pathSpec.findRecursive;
    this.extractPartition = pathSpec.extractPartition;
    this.properties = pathSpec.properties;
  }

  @JsonProperty
  public String getBasePath()
  {
    return basePath;
  }

  @JsonProperty
  public List<HynixPathSpecElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public Class getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public long getSplitSize()
  {
    return splitSize;
  }

  @JsonProperty
  public boolean isExtractPartition()
  {
    return extractPartition;
  }

  @JsonProperty
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    String schemaDataSource = config.getDataSource();
    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        Object value = entry.getValue();
        if (value == null) {
          job.getConfiguration().unset(entry.getKey());
          continue;
        }
        if (value instanceof String) {
          job.getConfiguration().set(entry.getKey(), (String) value);
        } else if (value instanceof Integer) {
          job.getConfiguration().setInt(entry.getKey(), (Integer) value);
        } else if (value instanceof Long) {
          job.getConfiguration().setLong(entry.getKey(), (Long) value);
        } else if (value instanceof Float) {
          job.getConfiguration().setFloat(entry.getKey(), (Float) value);
        } else if (value instanceof Double) {
          job.getConfiguration().setDouble(entry.getKey(), (Double) value);
        } else if (value instanceof Boolean) {
          job.getConfiguration().setBoolean(entry.getKey(), (Boolean) value);
        } else if (value instanceof List) {
          List<String> casted = GuavaUtils.cast((List<?>) value);
          job.getConfiguration().setStrings(entry.getKey(), casted.toArray(new String[casted.size()]));
        } else {
          new Logger(HynixPathSpec.class).warn("Invalid type value %s (%s).. ignoring", value, value.getClass());
        }
      }
    }

    List<String> paths = Lists.newArrayList();
    StringBuilder builder = new StringBuilder();
    for (HynixPathSpecElement element : elements) {
      String dataSource = Optional.fromNullable(element.getDataSource()).or(schemaDataSource);
      for (String path : HadoopGlobPathSplitter.splitGlob(element.getPaths())) {
        if (builder.length() > 0) {
          builder.append(',');
        }
        if (basePath != null) {
          path = basePath + "/" + path;
        }
        builder.append(dataSource).append(';').append(path);
        paths.add(path);
      }
    }
    // ds1;path1,ds1;path2,ds2;path3
    job.getConfiguration().set(PATH_SPECS, builder.toString());
    if (InputFormat.class.isAssignableFrom(inputFormat)) {
      job.getConfiguration().setClass(INPUT_FORMAT_NEW, inputFormat, InputFormat.class);
    } else if (org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormat)) {
      job.getConfiguration().setClass(INPUT_FORMAT_OLD, inputFormat, org.apache.hadoop.mapred.InputFormat.class);
    } else {
      throw new IllegalArgumentException("invalid format " + inputFormat);
    }
    job.getConfiguration().setLong(SPLIT_SIZE, splitSize);

    job.getConfiguration().setBoolean(FIND_RECURSIVE, findRecursive);
    job.getConfiguration().setBoolean(EXTRACT_PARTITION, extractPartition);

    // used for sized partition spec
    // path1;format1,path2;format2
    StaticPathSpec.addInputPath(job, paths, HynixCombineInputFormat.class);

    // should overwrite (DelegatingInputFormat is set in MultipleInputs.addInputPath)
    job.setInputFormatClass(HynixCombineInputFormat.class);

    return job;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HynixPathSpec that = (HynixPathSpec) o;

    if (!Objects.equals(basePath, that.basePath)) {
      return false;
    }
    if (!elements.equals(that.elements)) {
      return false;
    }
    if (!inputFormat.equals(that.inputFormat)) {
      return false;
    }
    if (splitSize != that.splitSize) {
      return false;
    }
    if (findRecursive != that.findRecursive) {
      return false;
    }
    if (extractPartition != that.extractPartition) {
      return false;
    }
    if (!Objects.equals(properties, that.properties)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(basePath, elements, inputFormat, splitSize, findRecursive, extractPartition);
  }

  @Override
  public String toString()
  {
    return "HynixPathSpec{" +
           "basePath=" + basePath +
           ", elements=" + elements +
           ", inputFormat=" + inputFormat +
           ", splitSize=" + splitSize +
           ", findRecursive=" + findRecursive +
           ", extractPartition=" + extractPartition +
           ", properties=" + properties +
           '}';
  }
}