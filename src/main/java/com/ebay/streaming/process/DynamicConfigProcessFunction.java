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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The process function which emit the config for each event in the source stream.
 *
 * @param <T> Event type of the source stream
 * @param <C> Config type application defined
 */
public class DynamicConfigProcessFunction<T, C> extends BroadcastProcessFunction<T, String, Tuple2<T, C>> {

    private FlinkDynamicConfigSettings<C> flinkGlobalConfigSettings;


    private static final String CLIENT_CONFIG_KEY = "clientConfigKey";

    private static final String POJO_SET_METHOD_PREFIX = "set";


    private final static Logger LOGGER = LoggerFactory.getLogger(DynamicConfigProcessFunction.class);


    public DynamicConfigProcessFunction(FlinkDynamicConfigSettings<C> flinkGlobalConfigSettings) {
        this.flinkGlobalConfigSettings = flinkGlobalConfigSettings;
    }

    /**
     * This will emit the pair of event in the source stream and the config
     *
     * @param t The stream element.
     * @param readOnlyContext A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *     querying the current processing/event time and updating the broadcast state. The context
     *     is only valid during the invocation of this method, do not store it.
     * @param collector The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(T t, BroadcastProcessFunction<T, String, Tuple2<T, C>>.ReadOnlyContext readOnlyContext,
                               Collector<Tuple2<T, C>> collector) throws Exception {
        try {
            String c = readOnlyContext.getBroadcastState(
                    new MapStateDescriptor<>(DynamicConfigConstants.GLOBAL_CONFIG_STATE_KEY, Types.STRING, Types.STRING)).get(CLIENT_CONFIG_KEY);

            LOGGER.info("Retrieve the stored config={}", c);

            if (StringUtils.isNotBlank(c)) {
                Map<String, Object> config = new Gson().fromJson(c, new TypeToken<Map<String, Object>>() {}.getType());

                Constructor<C> constructor = flinkGlobalConfigSettings.getDynamicConfigClassType().getConstructor();
                C configInstance = null;

                if (config != null) {
                    configInstance = constructor.newInstance();
                    for (Method method : flinkGlobalConfigSettings.getDynamicConfigClassType().getMethods()) {
                        if (method.getName().startsWith(POJO_SET_METHOD_PREFIX)) {
                            String methodNamePart = method.getName().substring(3);
                            String fieldName = methodNamePart.substring(0, 1).toLowerCase() + methodNamePart.substring(1);
                            Parameter param = method.getParameters()[0];
                            Object paramVal = config.get(fieldName);

                            LOGGER.info("PARAM_NAME={}, PARAM_TYPE={}, PARAM_VAL={}", fieldName, param.getType(), paramVal);
                            if (paramVal != null) {
                                if (param.getType() == Integer.class) {
                                    method.invoke(configInstance, Integer.valueOf((String) paramVal));
                                } else if (param.getType() == Long.class) {
                                    method.invoke(configInstance, Long.valueOf((String) paramVal));
                                } else if (param.getType() == String.class) {
                                    method.invoke(configInstance, (String) paramVal);
                                } else if (param.getType() == Boolean.class) {
                                    method.invoke(configInstance, Boolean.valueOf((String) paramVal));
                                } else if (param.getType() == List.class) {
                                    Field field = flinkGlobalConfigSettings.getDynamicConfigClassType().getDeclaredField(fieldName);
                                    Type type = field.getGenericType();
                                    if (type instanceof ParameterizedType) {
                                        ParameterizedType parameterizedType = (ParameterizedType) type;
                                        Type paramterType = parameterizedType.getActualTypeArguments()[0];

                                        Class<?> parameterClz = (Class<?>) paramterType;
                                        if (parameterClz == Integer.class) {
                                            List<Integer> vals = new ArrayList<>();
                                            for (Object item : (List) paramVal) {
                                                vals.add(Integer.valueOf((String) item));
                                            }
                                            method.invoke(configInstance, vals);
                                        } else if (parameterClz == String.class) {
                                            List<String> vals = new ArrayList<>();
                                            for (Object item : (List) paramVal) {
                                                vals.add((String) item);
                                            }
                                            method.invoke(configInstance, vals);
                                        } else if (parameterClz == Long.class) {
                                            List<Long> vals = new ArrayList<>();
                                            for (Object item : (List) paramVal) {
                                                vals.add(Long.valueOf((String) item));
                                            }
                                            method.invoke(configInstance, vals);
                                        } else if (parameterClz == Boolean.class) {
                                            List<Boolean> vals = new ArrayList<>();
                                            for (Object item : (List) paramVal) {
                                                vals.add(Boolean.valueOf((String) item));
                                            }
                                            method.invoke(configInstance, vals);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                collector.collect(new Tuple2<>(t, configInstance));
            } else {
                Constructor<C> ctor = flinkGlobalConfigSettings.getDynamicConfigClassType().getConstructor();
                collector.collect(new Tuple2<>(t, ctor.newInstance()));
            }
        } catch (Exception e) {
            LOGGER.info("Exception happened when emitting the global config", e);
        }
    }

    /**
     * In this method, the config will be stored in the MapState
     *
     * @param c The stream element.
     * @param context A {@link Context} that allows querying the timestamp of the element, querying the
     *     current processing/event time and updating the broadcast state. The context is only valid
     *     during the invocation of this method, do not store it.
     * @param collector The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String c, BroadcastProcessFunction<T, String, Tuple2<T, C>>.Context context,
                                        Collector<Tuple2<T, C>> collector) throws Exception {
        BroadcastState<String, String> state =
                context.getBroadcastState(new MapStateDescriptor<>(DynamicConfigConstants.GLOBAL_CONFIG_STATE_KEY, Types.STRING, Types.STRING));

        LOGGER.info("Capture the incoming global config={}", c);

        state.put(CLIENT_CONFIG_KEY, c);
    }
}
