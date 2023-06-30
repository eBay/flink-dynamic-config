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
package com.ebay.streaming.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockDataSink  extends RichSinkFunction<Tuple2<String, PlaygroundConfig>> {
    private static final List<Tuple2> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(Tuple2<String, PlaygroundConfig> value, SinkFunction.Context context) throws Exception {
        values.add(value);
    }

    public Tuple2<String, PlaygroundConfig> getLatestItem() {
        return values.get(values.size() - 1);
    }
}
