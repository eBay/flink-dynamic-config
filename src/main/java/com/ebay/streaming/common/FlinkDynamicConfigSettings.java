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

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class FlinkDynamicConfigSettings<C> implements Serializable {

    private static final long serialVersionUID = -1386340400622298677L;

    private String serviceName;

    private DynamicConfigSourceType dynamicConfigSourceType;
    private String dynamicConfigSourceEndpoint;

    private Class<C> dynamicConfigClassType;

    private Integer dynamicConfigPollingInterval;

    private Integer dynamicConfigPollingConnectTimeout;
    private Integer dynamicConfigPollingRequestTimeout;
    private Integer dynamicConfigPollingResponseTimeout;


    public DynamicConfigSourceType getDynamicConfigSourceType() {
        return dynamicConfigSourceType != null ? dynamicConfigSourceType : DynamicConfigSourceType.HTTP_POLLING;
    }

    public int getDynamicConfigPollingInterval() {
        return dynamicConfigPollingInterval != null ? dynamicConfigPollingInterval : 300;
    }

    public int getDynamicConfigPollingConnectTimeout() {
        return dynamicConfigPollingConnectTimeout != null ? dynamicConfigPollingConnectTimeout : 1;
    }

    public int getDynamicConfigPollingRequestTimeout() {
        return dynamicConfigPollingRequestTimeout != null ? dynamicConfigPollingRequestTimeout : 1;
    }

    public int getDynamicConfigPollingResponseTimeout() {
        return dynamicConfigPollingResponseTimeout != null ? dynamicConfigPollingResponseTimeout : 1;
    }
}
