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
package com.ebay.streaming.connector;

import com.ebay.streaming.common.FlinkDynamicConfigSettings;
import com.ebay.streaming.connector.http.HttpClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source connector to poll the config periodically.
 */
public class DynamicConfigHttpPollingSource extends RichParallelSourceFunction<String> {
    private volatile boolean running = true;

    private FlinkDynamicConfigSettings flinkGlobalConfigSettings;

    private final static Logger LOGGER = LoggerFactory.getLogger(DynamicConfigHttpPollingSource.class);

    public DynamicConfigHttpPollingSource(FlinkDynamicConfigSettings flinkGlobalConfigSettings) {
        this.flinkGlobalConfigSettings = flinkGlobalConfigSettings;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        running = true;
    }

    @Override
    public void run(SourceContext sourceContext) {
        while (running) {
            try {
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(flinkGlobalConfigSettings.getDynamicConfigPollingRequestTimeout()))
                        .setConnectTimeout(Timeout.ofSeconds(flinkGlobalConfigSettings.getDynamicConfigPollingConnectTimeout()))
                        .setResponseTimeout(Timeout.ofSeconds(flinkGlobalConfigSettings.getDynamicConfigPollingResponseTimeout()))
                        .build();
                String endpoint = Joiner.on("/").join(
                        flinkGlobalConfigSettings.getDynamicConfigSourceEndpoint(), flinkGlobalConfigSettings.getServiceName());

                HttpGet globalConfigRequest = new HttpGet(endpoint);
                globalConfigRequest.setConfig(requestConfig);
                CloseableHttpResponse httpResponse = HttpClientFactory.getInstance().getHttpClient().execute(globalConfigRequest);
                if (httpResponse.getCode() == HttpStatus.SC_OK) {
                    String content = EntityUtils.toString(httpResponse.getEntity());
                    LOGGER.info("GLOBAL_CONFIG={}", content);

                    sourceContext.collect(content);
                }

                Thread.sleep(flinkGlobalConfigSettings.getDynamicConfigPollingInterval() * 1000l);
            } catch (Throwable t) {
                LOGGER.error("Fail to find the global config", t);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
