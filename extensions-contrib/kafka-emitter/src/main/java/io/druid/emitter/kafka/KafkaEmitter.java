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

package io.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.utils.StringUtils;
import io.druid.emitter.kafka.MemoryBoundLinkedBlockingQueue.ObjectContainer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaEmitter implements Emitter
{
  private static Logger log = new Logger(KafkaEmitter.class);

  private static final int DEFAULT_RETRIES = 3;
  private final AtomicLong metricLost;
  private final AtomicLong alertLost;
  private final AtomicLong invalidLost;

  private final KafkaEmitterConfig config;
  private final Producer<Object, String> producer;
  private final Callback producerCallback;
  private final ObjectMapper jsonMapper;
  private final MemoryBoundLinkedBlockingQueue<String> metricQueue;
  private final MemoryBoundLinkedBlockingQueue<String> alertQueue;
  private final ScheduledExecutorService scheduler;

  public KafkaEmitter(KafkaEmitterConfig config, ObjectMapper jsonMapper)
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.producer = setKafkaProducer();
    this.producerCallback = setProducerCallback();
    // same with kafka producer's buffer.memory
    long queueMemoryBound = Long.parseLong(this.config.getKafkaProducerConfig()
                                                      .getOrDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"));
    this.metricQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.alertQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.scheduler = Executors.newScheduledThreadPool(3);
    this.metricLost = new AtomicLong(0L);
    this.alertLost = new AtomicLong(0L);
    this.invalidLost = new AtomicLong(0L);
  }

  private Callback setProducerCallback()
  {
    return new Callback()
    {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e)
      {
        if (e != null) {
          log.debug("Event send failed [%s]", e.getMessage());
          if (recordMetadata.topic().equals(config.getMetricTopic())) {
            metricLost.incrementAndGet();
          } else if (recordMetadata.topic().equals(config.getAlertTopic())) {
            alertLost.incrementAndGet();
          } else {
            invalidLost.incrementAndGet();
          }
        }
      }
    };
  }

  private Producer<Object, String> setKafkaProducer()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.RETRIES_CONFIG, DEFAULT_RETRIES);
      props.putAll(config.getKafkaProducerConfig());

      return new KafkaProducer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @LifecycleStart
  public void start()
  {
    scheduler.scheduleWithFixedDelay(
        new Runnable()
        {
          @Override
          public void run()
          {
            KafkaEmitter.this.sendMetricToKafka();
          }
        }, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(
        new Runnable()
        {
          @Override
          public void run()
          {
            KafkaEmitter.this.sendAlertToKafka();
          }
        }, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(
        new Runnable()
        {
          @Override
          public void run()
          {
            log.info(
                "Message lost counter: metricLost=[%d], alertLost=[%d], invalidLost=[%d]",
                metricLost.get(), alertLost.get(), invalidLost.get()
            );
          }
        }, 5, 5, TimeUnit.MINUTES);
    log.info("Starting Kafka Emitter.");
  }

  private void sendMetricToKafka()
  {
    sendToKafka(config.getMetricTopic(), metricQueue);
  }

  private void sendAlertToKafka()
  {
    sendToKafka(config.getAlertTopic(), alertQueue);
  }

  private void sendToKafka(final String topic, MemoryBoundLinkedBlockingQueue<String> recordQueue)
  {
    ObjectContainer<String> objectToSend;
    try {
      while (true) {
        objectToSend = recordQueue.take();
        producer.send(new ProducerRecord<>(topic, objectToSend.getData()), producerCallback);
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Failed to take record from queue!");
    }
  }

  @Override
  public void emit(final Event event)
  {
    if (event != null) {
      Map<String, Object> eventMap = new HashMap<>(event.toMap());
      eventMap.computeIfPresent("context", (String s, Object o) -> {
        if (o != null) {
          String contextStr = eventMap.get("context").toString();
          try {
            Map<String, Object> context = jsonMapper.readValue(contextStr, Map.class);
            return context;
          }
          catch (IOException e) {
            log.warn("Failed to parse context");
            return contextStr;
          }
        }
        return null;
      });

      ImmutableMap.Builder<String, Object> resultBuilder = ImmutableMap.<String, Object>builder().putAll(eventMap);
      if (config.getClusterName() != null) {
        resultBuilder.put("clusterName", config.getClusterName());
      }
      Map<String, Object> result = resultBuilder.build();

      try {
        String resultJson = jsonMapper.writeValueAsString(result);
        ObjectContainer<String> objectContainer = new ObjectContainer<>(
            resultJson,
            StringUtils.toUtf8(resultJson).length
        );
        if (event instanceof ServiceMetricEvent) {
          if (!metricQueue.offer(objectContainer)) {
            metricLost.incrementAndGet();
          }
        } else if (event instanceof AlertEvent) {
          if (!alertQueue.offer(objectContainer)) {
            alertLost.incrementAndGet();
          }
        } else {
          invalidLost.incrementAndGet();
        }
      }
      catch (JsonProcessingException e) {
        invalidLost.incrementAndGet();
      }
    }
  }

  @Override
  public void flush()
  {
    producer.flush();
  }

  @Override
  @LifecycleStop
  public void close()
  {
    scheduler.shutdownNow();
    producer.close();
  }
}
