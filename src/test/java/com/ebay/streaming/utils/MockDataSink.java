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
