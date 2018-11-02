package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ClassifyQuery extends BaseQuery<Object[]>
    implements Query.RewritingQuery<Object[]>, Query.ArrayOutputSupport<Object[]>
{
  private final Query<Object[]> query;
  private final Query<?> classifier;
  private final String tagColumn;

  public ClassifyQuery(
      @JsonProperty("query") Query<Object[]> query,
      @JsonProperty("classifier") Query<?> classifier,
      @JsonProperty("tagColumn") String tagColumn
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), query.getContext());
    this.query = Preconditions.checkNotNull(query, "'query' should not be null");
    this.classifier = Preconditions.checkNotNull(classifier, "'classifier' should not be null");
    this.tagColumn = Preconditions.checkNotNull(tagColumn, "'tagColumn' should not be null");
    Preconditions.checkArgument(query instanceof Query.ArrayOutputSupport, "'query' need to be array output");
    Preconditions.checkArgument(classifier instanceof ClassifierFactory, "'classifier' need to be classifier");
  }

  @Override
  public String getType()
  {
    return "classify";
  }

  @JsonProperty
  public Query<Object[]> getQuery()
  {
    return query;
  }

  @JsonProperty
  public Query<?> getClassifier()
  {
    return classifier;
  }

  @JsonProperty
  public String getTagColumn()
  {
    return tagColumn;
  }

  @Override
  public Query<Object[]> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new ClassifyQuery(query.withOverriddenContext(contextOverride), classifier, tagColumn);
  }

  @Override
  public Query<Object[]> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new ClassifyQuery(query.withQuerySegmentSpec(spec), classifier, tagColumn);
  }

  @Override
  public Query<Object[]> withDataSource(DataSource dataSource)
  {
    return new ClassifyQuery(query.withDataSource(dataSource), classifier, tagColumn);
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    if (query instanceof Query.ArrayOutputSupport) {
      List<String> outputColumns = ((Query.ArrayOutputSupport<?>) query).estimatedOutputColumns();
      if (outputColumns != null) {
        return GuavaUtils.concat(outputColumns, tagColumn);
      }
    }
    return null;
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    Query q = query.getId() == null ? query.withId(getId()) : query;
    if (q instanceof Query.RewritingQuery) {
      q = ((Query.RewritingQuery) q).rewriteQuery(segmentWalker, queryConfig, jsonMapper);
    }
    if (estimatedOutputColumns() == null && !PostProcessingOperators.isTabularOutput(q, jsonMapper)) {
      throw new IllegalArgumentException("cannot classify which is neither array output supported or tabular format");
    }
    Query c = classifier.getId() == null ? classifier.withId(getId()) : classifier;
    if (c instanceof Query.RewritingQuery) {
      c = ((Query.RewritingQuery) c).rewriteQuery(segmentWalker, queryConfig, jsonMapper);
    }
    Map<String, Object> postProcessing = ImmutableMap.<String, Object>of(
        QueryContextKeys.POST_PROCESSING, new ClassifyPostProcessor(tagColumn)
    );
    final Map<String, Object> context = computeOverriddenContext(postProcessing);
    return new UnionAllQuery(null, Arrays.asList(c, q), false, -1, 2, -1, context);
  }
}