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
package com.ebay.streaming.connector.http;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultRedirectStrategy;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

public class HttpClientFactory {
    private final static HttpClientFactory INSTANCE = new HttpClientFactory();

    private final CloseableHttpAsyncClient ASYNC_HTTP_CLIENT;

    private final CloseableHttpClient HTTP_CLIENT;

    private static final String DEFAULT_TLS_PROTOCOL = "TLSv1.2";

    private static final int READ_TIMEOUT = 3000;
    private static final int CONN_TIMEOUT = 1000;

    private static final int MAX_CONN = 50;
    private static final int MAX_CONN_PER_ROUTE = 50;

    private static final int CONN_TTL_IN_MIN = 5;

    private static final int IO_THREAD_NUM = 4;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    static class LooseTrustManager implements X509TrustManager {
        public static final LooseTrustManager INSTANCE =
                new LooseTrustManager();
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[0];
        }
        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
        }
        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
        }
    }

    private HttpClientFactory() {
        HTTP_CLIENT = getSyncHttpClient();
        ASYNC_HTTP_CLIENT = getHttpAsyncClient();
    }

    public static HttpClientFactory getInstance() {
        return INSTANCE;
    }

    public CloseableHttpAsyncClient getAsyncHttpClient() {
        return ASYNC_HTTP_CLIENT;
    }

    public CloseableHttpClient getHttpClient() {
        return HTTP_CLIENT;
    }

    private SSLContext getSSLContext(String protocol, boolean acceptAnyCertificate)
            throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext context = SSLContext.getInstance(protocol);;
        if (acceptAnyCertificate) {
            context.init(null,
                    new TrustManager[]{LooseTrustManager.INSTANCE}, new SecureRandom());
            context.getServerSessionContext().setSessionCacheSize(10000);
        } else {
            context.init(null, null, null);
        }
        return context;
    }

    private SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        return getSSLContext(DEFAULT_TLS_PROTOCOL, true);
    }

    private PoolingAsyncClientConnectionManager getPoolingAsyncClientConnectionManager() {
        try {
            final PoolingAsyncClientConnectionManager manager =  PoolingAsyncClientConnectionManagerBuilder.create()
                    .setTlsStrategy(ClientTlsStrategyBuilder.create()
                            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                            .setSslContext(getSSLContext()).build())
                    .setMaxConnTotal(MAX_CONN)
                    .setMaxConnPerRoute(MAX_CONN_PER_ROUTE)
                    .setConnectionTimeToLive(TimeValue.ofMinutes(CONN_TTL_IN_MIN))
                    .build();
            return manager;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            LOGGER.error("Failed to create PoolingAsyncClientConnectionManager", e);
        }
        return null;
    }

    private PoolingHttpClientConnectionManager getPoolingSyncClientConnectionManager() {
        PoolingHttpClientConnectionManager manager = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(MAX_CONN)
                .setMaxConnPerRoute(MAX_CONN_PER_ROUTE)
                .setConnectionTimeToLive(TimeValue.ofMinutes(CONN_TTL_IN_MIN))
                .build();
        return manager;
    }

    private RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(CONN_TIMEOUT))
                .setConnectionRequestTimeout(Timeout.ofMilliseconds(READ_TIMEOUT))
                .build();
    }

    private IOReactorConfig getIOReactorConfig() {
        return IOReactorConfig.custom()
                .setIoThreadCount(IO_THREAD_NUM)
                .setSoTimeout(CONN_TIMEOUT, TimeUnit.MILLISECONDS)
                .setSoKeepAlive(true).build();
    }

    private CloseableHttpAsyncClient getHttpAsyncClient() {
        HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom()
                .setConnectionManager(getPoolingAsyncClientConnectionManager())
                .setIOReactorConfig(getIOReactorConfig())
                .setDefaultRequestConfig(getRequestConfig())
                .setRedirectStrategy(DefaultRedirectStrategy.INSTANCE);
        CloseableHttpAsyncClient httpAsyncClient = httpAsyncClientBuilder.build();
        httpAsyncClient.start();
        return httpAsyncClient;
    }

    private CloseableHttpClient getSyncHttpClient() {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(getPoolingSyncClientConnectionManager())
                .setDefaultRequestConfig(getRequestConfig())
                .setRedirectStrategy(DefaultRedirectStrategy.INSTANCE).build();
        return httpClient;
    }
}