/*
 * Copyright 2023 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ebay.streaming.process;

import com.ebay.streaming.common.FlinkDynamicConfigSettings;
import com.ebay.streaming.common.DynamicConfigConstants;
import com.ebay.streaming.common.DynamicConfigSourceType;
import com.ebay.streaming.connector.DynamicConfigHttpPollingSource;
import lombok.Builder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.Map;
import java.util.Objects;

/**
 * This is to wire a source stream with a flink broadcast stream to build a new datastream. The event in the newly built
 * steam is the pair of the event coming from the source stream and the config. So in the new stream, application is able
 * to the source events with the config. The broadcast stream is receiving the config as events from the config source stream,
 * so the config is able to be dynamically updated if a config updated events in the config source stream.
 *
 * @param <T> Event type in the application source stream
 * @param <C> Config type
 */
public class DynamicConfigWire<T, C> {

    private final Map<DynamicConfigWire.GlobalConfigBroadStreamKey, BroadcastStream<T>> GLOBAL_CONFIG_STREAMS = Maps.newConcurrentMap();

    private final Map<DynamicConfigSourceType, SourceFunction<String>> GLOBAL_CONFIG_SOURCES;

    private FlinkDynamicConfigSettings<C> flinkGlobalConfigSettings;

    public DynamicConfigWire(FlinkDynamicConfigSettings<C> flinkGlobalConfigSettings) {
        this.flinkGlobalConfigSettings = flinkGlobalConfigSettings;
        this.GLOBAL_CONFIG_SOURCES = Maps.newConcurrentMap();
    }

    @Builder
    private static class GlobalConfigBroadStreamKey {
        private Class classOfConfig;
        private DynamicConfigSourceType globalConfigSourceType;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GlobalConfigBroadStreamKey that = (GlobalConfigBroadStreamKey) o;
            return Objects.equals(classOfConfig, that.classOfConfig) && globalConfigSourceType == that.globalConfigSourceType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classOfConfig, globalConfigSourceType);
        }
    }

    /**
     * This is to wire the config stream for the application data stream to have a new data stream with config for each event.
     * The event emitted in the new stream is like this: (raw event in the application stream, config).
     *
     * @param dataStream application datastream which is required to receive the config
     * @return
     */
    public SingleOutputStreamOperator<Tuple2<T, C>> wire(DataStream<T> dataStream) {
        GlobalConfigBroadStreamKey globalConfigBroadStreamKey = GlobalConfigBroadStreamKey.builder()
                .globalConfigSourceType(flinkGlobalConfigSettings.getDynamicConfigSourceType())
                .classOfConfig(flinkGlobalConfigSettings.getDynamicConfigClassType())
                .build();
        BroadcastStream globalConfigBroadcastStream = GLOBAL_CONFIG_STREAMS.get(globalConfigBroadStreamKey);
        if (globalConfigBroadcastStream == null) {
            MapStateDescriptor<String, String> globalConfigDescriptor =
                    new MapStateDescriptor<>(DynamicConfigConstants.GLOBAL_CONFIG_STATE_KEY, Types.STRING, Types.STRING);
            globalConfigBroadcastStream = dataStream.getExecutionEnvironment()
                    .addSource(getGlobalConfigSource(flinkGlobalConfigSettings.getDynamicConfigSourceType()))
                    .setParallelism(1)
                    .broadcast(globalConfigDescriptor);

            GLOBAL_CONFIG_STREAMS.put(globalConfigBroadStreamKey, globalConfigBroadcastStream);
        }
        return dataStream.connect(globalConfigBroadcastStream)
                .process(new DynamicConfigProcessFunction(flinkGlobalConfigSettings))
                .setParallelism(dataStream.getParallelism());
    }

    private SourceFunction<String> getGlobalConfigSource(DynamicConfigSourceType globalConfigSourceType) {
        SourceFunction<String> sourceFunction = GLOBAL_CONFIG_SOURCES.get(globalConfigSourceType) != null ? GLOBAL_CONFIG_SOURCES.get(globalConfigSourceType) : null;
        switch (globalConfigSourceType) {
            case HTTP_POLLING:
                if (sourceFunction == null) {
                    sourceFunction = new DynamicConfigHttpPollingSource(flinkGlobalConfigSettings);
                    GLOBAL_CONFIG_SOURCES.put(DynamicConfigSourceType.HTTP_POLLING, sourceFunction);
                }
                return sourceFunction;
            //TODO: support more Global config sources
        }
        throw new UnsupportedOperationException("Unsupported globalConfigSourceType=" + globalConfigSourceType);
    }

}