/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.druid.client.BrokerServerView;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.query.QueryInterruptedException;
import io.druid.server.QueryLifecycle;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.AuthorizationUtils;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link io.druid.server.QueryLifecycle}, this class manages the lifecycle of a SQL query.
 * It ensures that a SQL query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(String, Map)})</li>
 * <li>Planning ({@link #plan()})</li>
 * <li>Authorization ({@link #authorize(AuthorizationInfo)})</li>
 * <li>Execution ({@link #execute()})</li>
 * <li>Logging ({@link #emitLogsAndMetrics(Throwable, String, long)})</li>
 * </ol>
 *
 * <p>Unlike QueryLifecycle, this class is designed to be <b>thread safe</b> so that it can be used in multi-threaded
 * scenario (JDBC) without external synchronization.
 */
public class SqlLifecycle
{
  private static final Logger log = new Logger(SqlLifecycle.class);

  private final PlannerFactory plannerFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final AuthConfig authConfig;
  private final long startMs;
  private final long startNs;
  private final BrokerServerView brokerServerView;
  private final Object lock = new Object();

  private State state = State.NEW;

  // init during intialize
  private String sql;
  private Map<String, Object> queryContext;
  // init during plan
  @Nullable private HttpServletRequest req;
  private PlannerContext plannerContext;
  private PlannerResult plannerResult;

  public SqlLifecycle(
      PlannerFactory plannerFactory,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      final AuthConfig authConfig,
      long startMs,
      long startNs,
      BrokerServerView brokerServerView
  )
  {
    this.plannerFactory = plannerFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.authConfig = authConfig;
    this.startMs = startMs;
    this.startNs = startNs;
    this.brokerServerView = brokerServerView;
  }

  public String initialize(String sql, Map<String, Object> queryContext)
  {
    synchronized (lock) {
      transition(State.NEW, State.INITIALIZED);
      this.sql = sql;
      this.queryContext = contextWithSqlId(queryContext);
      return sqlQueryId();
    }
  }

  private Map<String, Object> contextWithSqlId(Map<String, Object> queryContext)
  {
    Map<String, Object> newContext = new HashMap<>();
    if (queryContext != null) {
      newContext.putAll(queryContext);
    }
    if (!newContext.containsKey(PlannerContext.CTX_SQL_QUERY_ID)) {
      newContext.put(PlannerContext.CTX_SQL_QUERY_ID, UUID.randomUUID().toString());
    }
    return newContext;
  }

  private String sqlQueryId()
  {
    return (String) this.queryContext.get(PlannerContext.CTX_SQL_QUERY_ID);
  }

  // TODO No AuthenticationResult
  public PlannerContext plan()
      throws ValidationException, RelConversionException, SqlParseException
  {
    synchronized (lock) {
      transition(State.INITIALIZED, State.PLANNED);
      try (DruidPlanner planner = plannerFactory.createPlanner(queryContext)) {
        this.plannerContext = planner.getPlannerContext();
        this.plannerResult = planner.plan(sql, brokerServerView);
      }
      return plannerContext;
    }
  }

  public RelDataType rowType()
  {
    synchronized (lock) {
      Preconditions.checkState(plannerResult != null,
                               "must be called after sql has been planned");
      return plannerResult.rowType();
    }
  }

  public Access authorize(@Nullable final AuthorizationInfo authorizationInfo)
  {

    if (authConfig.isEnabled()) {
      synchronized (lock) {
        transition(State.PLANNED, State.AUTHORIZING);

        Access accessAuth;
        if (authConfig.isEnabled()) {
          accessAuth = AuthorizationUtils.authorize(authorizationInfo, plannerResult.datasourceNames());
        } else {
          accessAuth = new Access(true);
        }
        return doAuthorize(accessAuth);
      }
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
      return new Access(true);
    }
  }

  private Access doAuthorize(final Access authorizationResult)
  {
    if (!authorizationResult.isAllowed()) {
      // Not authorized; go straight to Jail, do not pass Go.
      transition(State.AUTHORIZING, State.UNAUTHORIZED);
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
    }
    return authorizationResult;
  }

  // TODO seoeun
  public PlannerContext planAndAuthorize(final AuthorizationInfo authorizationInfo)
      throws SqlParseException, RelConversionException, ValidationException
  {
    PlannerContext plannerContext = plan();
    Access access = authorize(authorizationInfo);
    return plannerContext;
  }

  public Sequence<Object[]> execute()
  {
    synchronized (lock) {
      transition(State.AUTHORIZED, State.EXECUTING);
      return plannerResult.run();
    }
  }

  public Sequence<Object[]> runSimple(
      String sql,
      Map<String, Object> queryContext
  ) throws ValidationException, RelConversionException, SqlParseException
  {
    Sequence<Object[]> result;

    initialize(sql, queryContext);
    try {
      planAndAuthorize(null);
      result = execute();
      return result;
    }
    catch (Throwable e) {
      emitLogsAndMetrics(e, null, -1);
      throw e;
    }
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  public void emitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    synchronized (lock) {
      if (sql == null) {
        // Never initialized, don't log or emit anything.
        return;
      }

      if (state == State.DONE) {
        log.warn("Tried to emit logs and metrics twice for query[%s]!", sqlQueryId());
      }

      state = State.DONE;

      final boolean success = e == null;
      final long queryTimeNs = System.nanoTime() - startNs;

      try {
        ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
        if (plannerContext != null) {
          metricBuilder.setDimension("id", plannerContext.getSqlQueryId());
          //metricBuilder.setDimension("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
        }
        if (plannerResult != null) {
          metricBuilder.setDimension("dataSource", plannerResult.datasourceNames().toString());
        }
        metricBuilder.setDimension("remoteAddress", StringUtils.nullToEmptyNonDruidDataString(remoteAddress));
        metricBuilder.setDimension("success", String.valueOf(success));
        emitter.emit(metricBuilder.build("sqlQuery/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs)));
        if (bytesWritten >= 0) {
          emitter.emit(metricBuilder.build("sqlQuery/bytes", bytesWritten));
        }

        final Map<String, Object> statsMap = new LinkedHashMap<>();
        statsMap.put("sqlQuery/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs));
        statsMap.put("sqlQuery/bytes", bytesWritten);
        statsMap.put("success", success);
        statsMap.put("context", queryContext);
        if (plannerContext != null) {
          //queryContext.put("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
        }
        if (e != null) {
          statsMap.put("exception", e.toString());

          if (e instanceof QueryInterruptedException) {
            statsMap.put("interrupted", true);
            statsMap.put("reason", e.toString());
          }
        }

        requestLogger.logSqlQuery(
            new RequestLogLine(
                DateTimes.utc(startMs),
                remoteAddress,
                sql,
                new QueryStats(statsMap),
                queryContext
            )
        );
      }
      catch (Exception ex) {
        log.error(ex, "Unable to log sql [%s]!", sql);
      }
    }
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s] because current state[%s] is not [%s].", from, to, state, from);
    }

    state = to;
  }

  enum State
  {
    NEW,
    INITIALIZED,
    PLANNED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }
}
