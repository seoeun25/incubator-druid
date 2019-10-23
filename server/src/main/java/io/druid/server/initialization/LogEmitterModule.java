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

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.LoggingEmitter;
import io.druid.java.util.emitter.core.LoggingEmitterConfig;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;

/**
 */
public class LogEmitterModule implements Module
{
  public static final String EMITTER_TYPE = "logging";
  public static final String EVENT_EMITTER_TYPE = "event.logging";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.logging", LoggingEmitterConfig.class);
    JsonConfigProvider.bind(
        binder,
        "druid.event.emitter.logging",
        LoggingEmitterConfig.class,
        Names.named(EVENT_EMITTER_TYPE)
    );
  }

  @Provides @ManageLifecycle @Named(EMITTER_TYPE)
  public Emitter makeEmitter(Supplier<LoggingEmitterConfig> config, ObjectMapper jsonMapper)
  {
    return new LoggingEmitter(config.get(), jsonMapper);
  }

  @Provides @ManageLifecycle @Named(EVENT_EMITTER_TYPE)
  public Emitter makeEventEmitter(
      @Named(EVENT_EMITTER_TYPE) Supplier<LoggingEmitterConfig> config,
      ObjectMapper jsonMapper
  )
  {
    return new LoggingEmitter(config.get(), jsonMapper);
  }
}
