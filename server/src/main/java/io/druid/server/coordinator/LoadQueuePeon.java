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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.server.coordination.SegmentChangeRequestNoop;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(LoadQueuePeon.class);
  private static final int DROP = 0;
  private static final int LOAD = 1;

  private static void executeCallbacks(List<LoadPeonCallback> callbacks)
  {
    for (LoadPeonCallback callback : callbacks) {
      if (callback != null) {
        callback.execute();
      }
    }
  }

  private final CuratorFramework curator;
  private final String server;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService processingExecutor;
  private final ExecutorService callBackExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );

  private final Object lock = new Object();

  private volatile SegmentHolder currentlyProcessing = null;
  private boolean stopped = false;

  LoadQueuePeon(
      CuratorFramework curator,
      String loadQueuePath,
      String server,
      ObjectMapper jsonMapper,
      ScheduledExecutorService processingExecutor,
      ExecutorService callbackExecutor,
      DruidCoordinatorConfig config
  )
  {
    this.curator = curator;
    this.server = server;
    this.basePath = ZKPaths.makePath(loadQueuePath, server);
    this.jsonMapper = jsonMapper;
    this.callBackExecutor = callbackExecutor;
    this.processingExecutor = processingExecutor;
    this.config = config;
  }

  public String getBasePath()
  {
    return basePath;
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad.keySet();
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToDrop()
  {
    return segmentsToDrop.keySet();
  }

  public boolean isLoadingSegment(DataSegment segment)
  {
    return segmentsToLoad.containsKey(segment);
  }

  public boolean isDroppingSegment(DataSegment segment)
  {
    return segmentsToDrop.containsKey(segment);
  }

  public int getNumberOfQueuedSegments()
  {
    return segmentsToDrop.size() + segmentsToLoad.size();
  }

  public long getLoadQueueSize()
  {
    return queuedSize.get();
  }

  public int getAndResetFailedAssignCount()
  {
    return failedAssignCount.getAndSet(0);
  }

  public void loadSegment(
      final DataSegment segment,
      final LoadPeonCallback callback
  )
  {
    loadSegment(segment, "test", callback);
  }

  public void loadSegment(
      final DataSegment segment,
      final String loadReason,
      final LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyProcessing != null) &&
          currentlyProcessing.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyProcessing.addCallback(callback);
        }
        return;
      }
    }

    synchronized (lock) {
      final SegmentHolder existingHolder = segmentsToLoad.get(segment);
      if (existingHolder != null) {
        if ((callback != null)) {
          existingHolder.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server [%s] to load segment[%s] for [%s]", server, segment.getIdentifier(), loadReason);
    queuedSize.addAndGet(segment.getSize());
    segmentsToLoad.put(segment, new SegmentHolder(segment, LOAD, callback));
    doNext();
  }

  public void dropSegment(
      final DataSegment segment,
      final LoadPeonCallback callback
  )
  {
    dropSegment(segment, "test", callback);
  }

  public void dropSegment(
      final DataSegment segment,
      final String dropReason,
      final LoadPeonCallback callback
  )
  {
    synchronized (lock) {
      if ((currentlyProcessing != null) &&
          currentlyProcessing.getSegmentIdentifier().equals(segment.getIdentifier())) {
        if (callback != null) {
          currentlyProcessing.addCallback(callback);
        }
        return;
      }
    }

    synchronized (lock) {
      final SegmentHolder existingHolder = segmentsToDrop.get(segment);
      if (existingHolder != null) {
        if (callback != null) {
          existingHolder.addCallback(callback);
        }
        return;
      }
    }

    log.info("Asking server [%s] to drop segment[%s] for [%s]", server, segment.getIdentifier(), dropReason);
    segmentsToDrop.put(segment, new SegmentHolder(segment, DROP, callback));
    doNext();
  }

  private void doNext()
  {
    synchronized (lock) {
      if (currentlyProcessing == null) {
        if (!segmentsToDrop.isEmpty()) {
          currentlyProcessing = segmentsToDrop.firstEntry().getValue();
          log.debug("Server[%s] dropping [%s]", server, currentlyProcessing.getSegmentIdentifier());
        } else if (!segmentsToLoad.isEmpty()) {
          currentlyProcessing = segmentsToLoad.firstEntry().getValue();
          log.debug("Server[%s] loading [%s]", server, currentlyProcessing.getSegmentIdentifier());
        } else {
          return;
        }

        processingExecutor.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                synchronized (lock) {
                  try {
                    // expected when the coordinator looses leadership and LoadQueuePeon is stopped.
                    if (currentlyProcessing == null) {
                      if(!stopped) {
                        log.makeAlert("Crazy race condition! server[%s]", server)
                           .emit();
                      }
                      actionCompleted();
                      doNext();
                      return;
                    }
                    String identifier = currentlyProcessing.getSegmentIdentifier();
                    log.debug("Server[%s] processing segment[%s]", server, identifier);

                    final String path = ZKPaths.makePath(basePath, identifier);
                    final byte[] payload = jsonMapper.writeValueAsBytes(currentlyProcessing.getChangeRequest());
                    curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);

                    processingExecutor.schedule(
                        new Runnable()
                        {
                          @Override
                          public void run()
                          {
                            try {
                              if (curator.checkExists().forPath(path) != null) {
                                failAssign(new ISE("%s was never removed! Failing this operation!", path));
                              }
                            }
                            catch (Exception e) {
                              failAssign(e);
                            }
                          }
                        },
                        config.getLoadTimeoutDelay().getMillis(),
                        TimeUnit.MILLISECONDS
                    );

                    final Stat stat = curator.checkExists().usingWatcher(
                        new CuratorWatcher()
                        {
                          @Override
                          public void process(WatchedEvent watchedEvent) throws Exception
                          {
                            switch (watchedEvent.getType()) {
                              case NodeDeleted:
                                entryRemoved(watchedEvent.getPath());
                            }
                          }
                        }
                    ).forPath(path);

                    if (stat == null) {
                      final byte[] noopPayload = jsonMapper.writeValueAsBytes(new SegmentChangeRequestNoop());

                      // Create a node and then delete it to remove the registered watcher.  This is a work-around for
                      // a zookeeper race condition.  Specifically, when you set a watcher, it fires on the next event
                      // that happens for that node.  If no events happen, the watcher stays registered foreverz.
                      // Couple that with the fact that you cannot set a watcher when you create a node, but what we
                      // want is to create a node and then watch for it to get deleted.  The solution is that you *can*
                      // set a watcher when you check to see if it exists so, we first create the node and then set a
                      // watcher on its existence.  However, if already does not exist by the time the existence check
                      // returns, then the watcher that was set will never fire (nobody will ever create the node
                      // again) and thus lead to a slow, but real, memory leak.  So, we create another node to cause
                      // that watcher to fire and delete it right away.
                      //
                      // We do not create the existence watcher first, because then it will fire when we create the
                      // node and we'll have the same race when trying to refresh that watcher.
                      curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, noopPayload);

                      entryRemoved(path);
                    }
                  }
                  catch (Exception e) {
                    failAssign(e);
                  }
                }
              }
            }
        );
      } else {
        log.debug(
            "Server[%s] skipping doNext() because something is currently loading[%s].",
            server,
            currentlyProcessing.getSegmentIdentifier()
        );
      }
    }
  }

  private void actionCompleted()
  {
    if (currentlyProcessing != null) {
      switch (currentlyProcessing.getType()) {
        case LOAD:
          segmentsToLoad.remove(currentlyProcessing.getSegment());
          queuedSize.addAndGet(-currentlyProcessing.getSegmentSize());
          break;
        case DROP:
          segmentsToDrop.remove(currentlyProcessing.getSegment());
          break;
        default:
          throw new UnsupportedOperationException();
      }

      final List<LoadPeonCallback> callbacks = currentlyProcessing.getCallbacks();
      currentlyProcessing = null;
      callBackExecutor.execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              executeCallbacks(callbacks);
            }
          }
      );
    }
  }

  public void stop()
  {
    synchronized (lock) {
      if (currentlyProcessing != null) {
        executeCallbacks(currentlyProcessing.getCallbacks());
        currentlyProcessing = null;
      }

      if (!segmentsToDrop.isEmpty()) {
        for (SegmentHolder holder : segmentsToDrop.values()) {
          executeCallbacks(holder.getCallbacks());
        }
      }
      segmentsToDrop.clear();

      if (!segmentsToLoad.isEmpty()) {
        for (SegmentHolder holder : segmentsToLoad.values()) {
          executeCallbacks(holder.getCallbacks());
        }
      }
      segmentsToLoad.clear();

      queuedSize.set(0L);
      failedAssignCount.set(0);
      stopped = true;
    }
  }

  private void entryRemoved(String path)
  {
    synchronized (lock) {
      if (currentlyProcessing == null) {
        log.debug("Server[%s] an entry[%s] was removed even though it wasn't loading!?", server, path);
        return;
      }
      if (!ZKPaths.getNodeFromPath(path).equals(currentlyProcessing.getSegmentIdentifier())) {
        log.debug(
            "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
            server, path, currentlyProcessing
        );
        return;
      }
      actionCompleted();
      log.debug("Server[%s] done processing [%s]", basePath, path);
    }

    doNext();
  }

  private void failAssign(Exception e)
  {
    synchronized (lock) {
      log.debug(e, "Server[%s], throwable caught when submitting [%s].", server, currentlyProcessing);
      failedAssignCount.getAndIncrement();
      // Act like it was completed so that the coordinator gives it to someone else
      actionCompleted();
      doNext();
    }
  }

  private static class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final int type;
    private final List<LoadPeonCallback> callbacks = Lists.newArrayList();

    private SegmentHolder(DataSegment segment, int type, LoadPeonCallback callback)
    {
      this.segment = segment;
      this.type = type;
      this.changeRequest = (type == LOAD)
                           ? new SegmentChangeRequestLoad(segment)
                           : new SegmentChangeRequestDrop(segment);
      if (callback != null) {
        this.callbacks.add(callback);
      }
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public int getType()
    {
      return type;
    }

    public String getSegmentIdentifier()
    {
      return segment.getIdentifier();
    }

    public long getSegmentSize()
    {
      return segment.getSize();
    }

    public void addCallbacks(Collection<LoadPeonCallback> newCallbacks)
    {
      synchronized (callbacks) {
        callbacks.addAll(newCallbacks);
      }
    }

    public void addCallback(LoadPeonCallback newCallback)
    {
      synchronized (callbacks) {
        callbacks.add(newCallback);
      }
    }

    public List<LoadPeonCallback> getCallbacks()
    {
      synchronized (callbacks) {
        return callbacks;
      }
    }

    public DataSegmentChangeRequest getChangeRequest()
    {
      return changeRequest;
    }

    @Override
    public String toString()
    {
      return changeRequest.toString();
    }
  }
}
