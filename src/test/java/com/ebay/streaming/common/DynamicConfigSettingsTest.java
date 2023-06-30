package com.ebay.streaming.common;

import com.ebay.streaming.utils.PlaygroundConfig;
import org.junit.Assert;
import org.junit.Test;

public class DynamicConfigSettingsTest {

    @Test
    public void build() {

        FlinkDynamicConfigSettings<PlaygroundConfig> flinkDynamicConfigSettings = FlinkDynamicConfigSettings.<PlaygroundConfig>builder()
                .dynamicConfigSourceType(DynamicConfigSourceType.HTTP_POLLING)
                .dynamicConfigSourceEndpoint("http://localhost:8080/v1/globalconfig/latest4flink")
                .dynamicConfigClassType(PlaygroundConfig.class)
                .build();
        Assert.assertEquals(flinkDynamicConfigSettings.getDynamicConfigSourceType(),DynamicConfigSourceType.HTTP_POLLING);
        Assert.assertNotNull(flinkDynamicConfigSettings.getDynamicConfigClassType());

    }
}
