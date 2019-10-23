/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.server.http;

import com.google.common.base.Throwables;
import io.druid.java.util.common.IAE;
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.Self;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.server.DruidNode;
import io.druid.server.Shutdown;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.eclipse.jetty.io.EofException;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Path("/druid/admin")
public class AdminResource
{
  private final DruidNode node;
  private final Shutdown.Proc shutdown;

  @Inject
  public AdminResource(@Self DruidNode node, @Shutdown Shutdown.Proc shutdown)
  {
    this.node = node;
    this.shutdown = shutdown;
  }

  @GET
  @Path("/ping")
  @Produces(MediaType.APPLICATION_JSON)
  public Response ping()
  {
    return Response.ok().build();
  }

  @GET
  @Path("/jmx")
  @Produces(MediaType.APPLICATION_JSON)
  public Response jmx()
  {
    final Map<String, Object> entity = JMXQueryRunnerFactory.queryJMX(node, null, false);
    return Response.ok(entity.get(node.getHostAndPort())).build();
  }

  @GET
  @Path("/stack")
  @Produces(MediaType.TEXT_PLAIN)
  public Response stack()
  {
    final StreamingOutput output = new StreamingOutput()
    {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException
      {
        final OutputStreamWriter writer = new OutputStreamWriter(output, StringUtils.UTF8_STRING);
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        for (long threadId : threadMXBean.getAllThreadIds()) {
          writer.write(dump(threadMXBean.getThreadInfo(threadId, Integer.MAX_VALUE)));
        }
        writer.flush();
      }
    };
    return Response.ok(output, MediaType.TEXT_PLAIN).build();
  }

  // copied from ThreadInfo.toString() to dump all stack traces
  private static String dump(ThreadInfo thread)
  {
    StringBuilder sb = new StringBuilder(1024);

    sb.append('\"').append(thread.getThreadName()).append('\"')
      .append(" Id=").append(thread.getThreadId()).append(' ').append(thread.getThreadState());

    if (thread.getLockName() != null) {
      sb.append(" on ").append(thread.getLockName());
    }
    if (thread.getLockOwnerName() != null) {
      sb.append(" owned by \"").append(thread.getLockOwnerName()).append("\" Id=").append(thread.getLockOwnerId());
    }
    if (thread.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (thread.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');

    StackTraceElement[] stackTrace = thread.getStackTrace();
    for (int i = 0; i < stackTrace.length; i++) {
      StackTraceElement ste = stackTrace[i];
      sb.append("\tat ").append(ste.toString());
      sb.append('\n');
      if (i == 0 && thread.getLockInfo() != null) {
        Thread.State ts = thread.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on ").append(thread.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on ").append(thread.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on ").append(thread.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : thread.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked ").append(mi);
          sb.append('\n');
        }
      }
    }

    LockInfo[] locks = thread.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = ").append(locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- ").append(li);
        sb.append('\n');
      }
    }
    sb.append('\n');
    return sb.toString();
  }

  @GET
  @Path("/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public Response shutdown(@QueryParam("timeout") Long timeout) throws IOException
  {
    if (timeout == null || timeout < 0) {
      shutdown.shutdown();
    } else {
      shutdown.shutdown(timeout);
    }
    return Response.ok().build();
  }

  @GET
  @Path("/logLevel/{name}/{level}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response logLevel(@PathParam("name") String name, @PathParam("level") String levelString)
  {
    Level level = Level.toLevel(levelString, null);
    if (level == null) {
      throw new IAE("Invalid level [%s]", levelString);
    }
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Logger logger = "root".equalsIgnoreCase(name) ? context.getRootLogger() : context.getLogger(name);
    logger.setLevel(level);
    return Response.ok().build();
  }

  private static final int DEFAULT_LOG_QUEUE_SIZE = 1024;
  private static final int DEFAULT_LOG_POLLING_SEC = 300;   // 5 min

  @GET
  @Path("/log")
  @Produces(MediaType.TEXT_PLAIN)
  public Response log(
      @QueryParam("level") String level,
      @QueryParam("timeout") Integer timeout
  )
  {
    return Response.ok(streamLog(null, level, timeout), MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/log/{name}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response log(
      @PathParam("name") String name,
      @QueryParam("level") String level,
      @QueryParam("timeout") Integer timeout
  )
  {
    return Response.ok(streamLog(name, level, timeout), MediaType.TEXT_PLAIN).build();
  }

  private StreamingOutput streamLog(final String name, final String level, final Integer timeout)
  {
    final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    final Logger logger = StringUtils.isNullOrEmpty(name) ? context.getRootLogger() : context.getLogger(name);

    final Filter filter = toLevelFilter(level);
    final long polling = TimeUnit.SECONDS.toMillis(timeout == null || timeout <= 0 ? DEFAULT_LOG_POLLING_SEC : timeout);

    return new StreamingOutput()
    {
      @Override
      public void write(final OutputStream output) throws IOException, WebApplicationException
      {
        final BlockingQueue<LogEvent> queue = new LinkedBlockingDeque<>(DEFAULT_LOG_QUEUE_SIZE);
        final Appender appender = new AbstractAppender(UUID.randomUUID().toString(), filter, null)
        {
          @Override
          public void append(LogEvent event)
          {
            queue.offer(event);   // can leak
          }
        };
        logger.addAppender(appender);
        appender.start();

        final Layout<String> layout = getLayout(logger);
        final long limit = System.currentTimeMillis() + polling;
        try {
          for (long remaining = polling; remaining > 0; remaining = limit - System.currentTimeMillis()) {
            final LogEvent event = queue.poll(remaining, TimeUnit.MILLISECONDS);
            if (event == null) {
              break;
            }
            output.write(layout.toByteArray(event));
            if (queue.isEmpty()) {
              output.flush();
            }
          }
        }
        catch (Throwable t) {
          if (!(t instanceof EofException)) {
            throw Throwables.propagate(t);
          }
        }
        finally {
          logger.removeAppender(appender);
          appender.stop();
        }
      }
    };
  }

  private Filter toLevelFilter(String levelString)
  {
    final Level level = Level.toLevel(levelString, null);
    if (level == null) {
      return null;
    }
    return new AbstractFilter()
    {
      @Override
      public Result filter(LogEvent event)
      {
        return level.isLessSpecificThan(event.getLevel()) ? Result.ACCEPT : Result.DENY;
      }
    };
  }

  private Layout<String> getLayout(Logger rootLogger)
  {
    for (Appender appender : rootLogger.getAppenders().values()) {
      if (appender.getLayout() instanceof AbstractStringLayout) {
        return (AbstractStringLayout) appender.getLayout();
      }
    }
    return PatternLayout.createDefaultLayout();
  }
}
