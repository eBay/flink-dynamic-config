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
