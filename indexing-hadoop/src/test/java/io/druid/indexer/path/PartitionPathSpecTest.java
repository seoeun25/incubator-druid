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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.Interval;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;

public class PartitionPathSpecTest
{
  private PartitionPathSpec partitionPathSpec;
  private final String TEST_STRING_BASE_PATH = "TEST";
  private final List<String> TEST_STRING_PARTITION_COLS = ImmutableList.of("test1", "test2");
  private final List<String> TEST_STRING_SINGLE_PARTITON_COLS = ImmutableList.of("test2");

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Rule
  public final TemporaryFolder testFolder = new TemporaryFolder();

  @Before
  public void setUp()
  {
    partitionPathSpec = new PartitionPathSpec();
  }

  @After
  public void tearDown()
  {
    partitionPathSpec = null;
  }

  @Test
  public void testSetBasePathString()
  {
    partitionPathSpec.setBasePath(TEST_STRING_BASE_PATH);
    Assert.assertEquals(TEST_STRING_BASE_PATH, partitionPathSpec.getBasePath());
  }

  @Test
  public void testSetPartitionColumns()
  {
    partitionPathSpec.setPartitionColumns(TEST_STRING_PARTITION_COLS);
    Assert.assertEquals(TEST_STRING_PARTITION_COLS, partitionPathSpec.getPartitionColumns());
  }

  @Test
  public void testSerdeCustomInputFormat() throws Exception
  {
    testSerde("/test/path", ImmutableList.of("test1", "test2"), ImmutableList.of("/test/path/a", "test/path/b"), TextInputFormat.class);
  }

  @Test
  public void testSerdeNoInputFormat() throws Exception
  {
    testSerde("/test/path", ImmutableList.of("test1", "test2"), ImmutableList.of("/test/path/a", "test/path/b"), null);
  }

  @Test
  public void testAddInputPath() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    partitionPathSpec.setPartitionColumns(TEST_STRING_PARTITION_COLS);
    partitionPathSpec.setInputFormat(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFolder("test", "test1=def", "test2=123");
    testFolder.newFolder("test", "test1=def", "testz=123");
    testFolder.newFolder("test", "testz=def", "test2=123");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");
    testFolder.newFile("test/test1=def/test2=123/file3");
    testFolder.newFile("test/test1=def/test2=123/file4");
    testFolder.newFile("test/test1=def/testz=123/file4");
    testFolder.newFile("test/testz=def/test2=123/file4");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test");

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=def/test2=123")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test1", "abc",
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  @Test
  public void testAddInputPathSubPartition() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    partitionPathSpec.setPartitionColumns(TEST_STRING_SINGLE_PARTITON_COLS);
    partitionPathSpec.setInputFormat(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test/test1=abc");

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  @Test
  public void testAddInputPathFullPartition() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    partitionPathSpec.setPartitionColumns(TEST_STRING_SINGLE_PARTITON_COLS);
    partitionPathSpec.setInputFormat(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test/test1=abc");
    partitionPathSpec.setIndexingPaths(
        ImmutableList.of(
            testFolder.getRoot().getPath() + "/test/test1=abc/test2=123",
            testFolder.getRoot().getPath() + "/test/test1=abc/test2=456"
        )
    );

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  @Test
  public void testAddInputPathPartialScan() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    partitionPathSpec.setPartitionColumns(TEST_STRING_PARTITION_COLS);
    partitionPathSpec.setInputFormat(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFolder("test", "test1=def", "test2=123");
    testFolder.newFolder("test", "test1=def", "testz=123");
    testFolder.newFolder("test", "testz=def", "test2=123");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");
    testFolder.newFile("test/test1=def/test2=123/file3");
    testFolder.newFile("test/test1=def/test2=123/file4");
    testFolder.newFile("test/test1=def/testz=123/file4");
    testFolder.newFile("test/testz=def/test2=123/file4");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test");
    partitionPathSpec.setIndexingPaths(
        ImmutableList.of(
            testFolder.getRoot().getPath() + "/test/test1=abc",
            testFolder.getRoot().getPath() + "/test/test1=def"
        )
    );

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=def/test2=123")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test1", "abc",
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  @Test
  public void testAddInputPathWithNoPartitionColumns() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFolder("test", "test1=def", "test2=123");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");
    testFolder.newFile("test/test1=def/test2=123/file3");
    testFolder.newFile("test/test1=def/test2=123/file4");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test");

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=def/test2=123")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test1", "abc",
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  @Test
  public void testAddInputPathWithNoPartitionColumnsSubPartition() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test/test1=abc");

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  @Test
  public void testAddInputPathWithNoPartitionColumnsPartialScan() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(null, null, null, null, null, null, false, false, false, false, null, false, false, null, null, null)
    );

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "test1=abc", "test2=123");
    testFolder.newFolder("test", "test1=abc", "test2=456");
    testFolder.newFolder("test", "test1=def", "test2=123");
    testFolder.newFile("test/test1=abc/test2=123/file1");
    testFolder.newFile("test/test1=abc/test2=456/file2");
    testFolder.newFile("test/test1=def/test2=123/file3");
    testFolder.newFile("test/test1=def/test2=123/file4");

    partitionPathSpec.setBasePath(testFolder.getRoot().getPath() + "/test");
    partitionPathSpec.setIndexingPaths(
        ImmutableList.of(
            testFolder.getRoot().getPath() + "/test/test1=abc",
            testFolder.getRoot().getPath() + "/test/test1=def"
        )
    );

    partitionPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=123"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=abc/test2=456"),
        String.format(formatStr, testFolder.getRoot(), "test/test1=def/test2=123")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);

    Map<String, String> columnsExpected = ImmutableMap.of(
        "test1", "abc",
        "test2", "123"
    );

    Map<String, String> partitionColumns = partitionPathSpec.getPartitionValues(
        new Path(String.format("file:%s/test/test1=abc/test2=123/file1",testFolder.getRoot()))
    );

    Assert.assertEquals(columnsExpected, partitionColumns);
  }

  private void testSerde(
      String basePath,
      List<String> indexingPaths,
      List<String> partitionColumns,
      Class inputFormat) throws Exception
  {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"basePath\" : \"");
    sb.append(basePath);
    sb.append("\",");
    sb.append("\"indexingPaths\" : [\"");
    sb.append(StringUtils.join(indexingPaths, "\", \""));
    sb.append("\"],");
    sb.append("\"partitionColumns\" : [\"");
    sb.append(StringUtils.join(partitionColumns, "\", \""));
    sb.append("\"],");
    if(inputFormat != null) {
      sb.append("\"inputFormat\" : \"");
      sb.append(inputFormat.getName());
      sb.append("\",");
    }
    sb.append("\"type\" : \"partition\"}");

    PartitionPathSpec pathSpec = (PartitionPathSpec) StaticPathSpecTest.readWriteRead(sb.toString(), jsonMapper);
    Assert.assertEquals(inputFormat, pathSpec.getInputFormat());
    Assert.assertEquals(basePath, pathSpec.getBasePath());
    Assert.assertEquals(partitionColumns, pathSpec.getPartitionColumns());
  }
}