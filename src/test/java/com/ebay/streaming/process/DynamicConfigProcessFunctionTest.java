package com.ebay.streaming.process;

import com.ebay.streaming.utils.MockDataSink;
import com.ebay.streaming.utils.MockDataSource;
import com.ebay.streaming.utils.PlaygroundConfig;
import com.ebay.streaming.common.DynamicConfigSourceType;
import com.ebay.streaming.common.FlinkDynamicConfigSettings;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;

public class DynamicConfigProcessFunctionTest {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(6200);

    @Test
    public void testProcessFunction() throws Exception {
        String config = "{ \"intVal\":\"1\", \"longVal\":\"2\", \"strVal\":\"This is a play ground\",\"boolVal\":\"true\" }";

        stubFor(get(urlPathMatching("/dynamic-configs/.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(config)));

        FlinkDynamicConfigSettings flinkDynamicConfigSettings = FlinkDynamicConfigSettings.<PlaygroundConfig>builder()
                .serviceName("flink-playground-java")
                .dynamicConfigSourceEndpoint("http://localhost:6200/dynamic-configs/flink-playground-java")
                .dynamicConfigSourceType(DynamicConfigSourceType.HTTP_POLLING)
                .dynamicConfigPollingInterval(1)
                .dynamicConfigClassType(PlaygroundConfig.class)
                .build();

        DynamicConfigWire<String, PlaygroundConfig> globalConfigWire = new DynamicConfigWire<>(flinkDynamicConfigSettings);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> sourceDataStream = env
                .addSource(new MockDataSource()).setParallelism(1);

        MockDataSink mockDataSink = new MockDataSink();
        globalConfigWire.wire(sourceDataStream)
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, PlaygroundConfig>>(){}))
                .addSink(mockDataSink).setParallelism(1);

        CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> {
            try {
                env.execute();
            } catch (Exception e) {}
        });
        try {
            completableFuture.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            completableFuture.cancel(true);
        }

        Tuple2<String, PlaygroundConfig> lastOne = mockDataSink.getLatestItem();

        String sourceItem = lastOne.f0;
        PlaygroundConfig playgroundConfig = lastOne.f1;

        Assert.assertNotNull(sourceItem);
        Assert.assertEquals(Integer.valueOf(1), playgroundConfig.getIntVal());
        Assert.assertEquals(Long.valueOf(2), playgroundConfig.getLongVal());
        Assert.assertEquals("This is a play ground", playgroundConfig.getStrVal());
        Assert.assertEquals(Boolean.TRUE, playgroundConfig.getBoolVal());

    }

}
