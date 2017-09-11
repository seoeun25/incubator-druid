package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.IngestionMode;
import io.druid.segment.indexing.DataSchema;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class HynixIndexTask extends HadoopIndexTask
{
  @SuppressWarnings("unchecked")
  private String extractRequiredLockName(HadoopIngestionSpec spec)
  {
    DataSchema dataSchema = spec.getDataSchema();
    String schemaDataSource = dataSchema.getDataSource();
    Map<String, Object> pathSpec = spec.getIOConfig().getPathSpec();
    if ("hynix".equals(pathSpec.get("type"))) {
      // simple validation
      HadoopTuningConfig tuningConfig = spec.getTuningConfig();
      if (tuningConfig.getIngestionMode() != IngestionMode.REDUCE_MERGE) {
        throw new IllegalArgumentException("hynix type input spec only can be used with REDUCE_MERGE mode");
      }
      Set<String> dataSources = Sets.newLinkedHashSet();
      for (Map elementSpec : (List<Map>) pathSpec.get("elements")) {
        String dataSourceName = Objects.toString(elementSpec.get("dataSource"), schemaDataSource);
        if (dataSourceName == null || dataSourceName.indexOf(';') >= 0) {
          throw new IllegalArgumentException("Datasource name should not be empty or contain ';'");
        }
        if (!dataSources.contains(dataSourceName)) {
          dataSources.add(dataSourceName);
        }
      }
      if (dataSchema.getGranularitySpec().isAppending()) {
        Preconditions.checkArgument(dataSources.size() == 1, "cannot append on multi datasources, for now");
      }
      return StringUtils.join(dataSources, ';');
    }
    return schemaDataSource;
  }

  @JsonIgnore
  private final String requiredLockName;

  public HynixIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") HadoopIngestionSpec spec,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? createNewId("index_hynix", spec) : id,
        spec,
        hadoopCoordinates,
        hadoopDependencyCoordinates,
        classpathPrefix,
        jsonMapper,
        context
    );
    this.requiredLockName = extractRequiredLockName(spec);
  }

  @Override
  public String getType()
  {
    return "index_hynix";
  }

  @Override
  public String getRequiredLockName()
  {
    return requiredLockName;
  }
}
