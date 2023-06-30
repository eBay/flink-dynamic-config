package com.ebay.streaming.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.experimental.theories.Theories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class MockDataSource extends RichParallelSourceFunction<String> {


    private static final String[] MOCK_DATA = {"hello", "world", "flink", "java", "dynamic", "config"};

    private static final Random RAND = new Random();

    private Logger log = LoggerFactory.getLogger(MockDataSource.class);

    public transient boolean running;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        running = true;
    }

    @Override
    public void run(SourceContext sourceContext) {
        while (running) {
            String data = MOCK_DATA[RAND.nextInt(MOCK_DATA.length)];
            log.info("IN_MOCK_DATA_SOURCE, SUB_TASK={}, SOURCE_DATA={}", getRuntimeContext().getIndexOfThisSubtask(), data);
            sourceContext.collect(data);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {}
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
