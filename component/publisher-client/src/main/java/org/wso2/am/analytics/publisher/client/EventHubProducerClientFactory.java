/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.am.analytics.publisher.client;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.amqp.ProxyAuthenticationType;
import com.azure.core.amqp.ProxyOptions;
import com.azure.core.credential.TokenCredential;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.am.analytics.publisher.auth.AuthClient;
import org.wso2.am.analytics.publisher.exception.ConnectionRecoverableException;
import org.wso2.am.analytics.publisher.exception.ConnectionUnrecoverableException;
import org.wso2.am.analytics.publisher.util.Constants;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URLDecoder;
import java.util.Map;

/**
 * Factory class to create EventHubProducerClient instance.
 */
public class EventHubProducerClientFactory {
    private static final Logger log = LogManager.getLogger(EventHubClient.class);

    public static EventHubProducerClient create(String authEndpoint, String authToken, AmqpRetryOptions retryOptions,
                                                Map<String, String> properties)
            throws ConnectionRecoverableException, ConnectionUnrecoverableException {
        log.info("Creating EventHub producer client");
        TokenCredential tokenCredential = new WSO2TokenCredential(authEndpoint, authToken, properties);
        String tempSASToken;
        // generate SAS token to get eventhub meta data
        log.debug("Generating SAS token for EventHub metadata");
        tempSASToken = getSASToken(authEndpoint, authToken, properties);

        String resourceURI = getResourceURI(tempSASToken);
        String fullyQualifiedNamespace = getNamespace(resourceURI);
        String eventhubName = getEventHubName(resourceURI);
        if (log.isDebugEnabled()) {
            log.debug("Extracted EventHub details - Namespace: " + fullyQualifiedNamespace + ", EventHub: " 
                    + eventhubName);
        }

        String isProxyEnabled = properties.get(Constants.PROXY_ENABLE);
        if (Boolean.parseBoolean(isProxyEnabled)) {
            log.info("Configuring EventHub client with proxy settings");
            String proxyHost = properties.get(Constants.PROXY_HOST);
            int proxyPort = Integer.parseInt(properties.get(Constants.PROXY_PORT));
            String proxyUsername = properties.get(Constants.PROXY_USERNAME);
            String proxyPassword = properties.get(Constants.PROXY_PASSWORD);

            if (log.isDebugEnabled()) {
                log.debug("Proxy configuration - Host: " + proxyHost + ", Port: " + proxyPort + ", Auth: " 
                        + (!StringUtils.isBlank(proxyUsername) ? "BASIC" : "NONE"));
            }

            SocketAddress address = new InetSocketAddress(proxyHost, proxyPort);
            Proxy proxyAddress = new Proxy(Proxy.Type.HTTP, address);
            ProxyOptions proxyOptions;
            if (!StringUtils.isBlank(proxyUsername) && !StringUtils.isBlank(proxyPassword)) {
                log.debug("Setting up proxy with BASIC authentication");
                proxyOptions =
                        new ProxyOptions(ProxyAuthenticationType.BASIC, proxyAddress, proxyUsername, proxyPassword);
            } else {
                log.debug("Setting up proxy without authentication");
                proxyOptions =
                        new ProxyOptions(ProxyAuthenticationType.NONE, proxyAddress, null, null);
            }

            EventHubProducerClient client = new EventHubClientBuilder()
                    .credential(fullyQualifiedNamespace, eventhubName, tokenCredential)
                    .proxyOptions(proxyOptions)
                    .retry(retryOptions)
                    .transportType(AmqpTransportType.AMQP_WEB_SOCKETS)
                    .buildProducerClient();
            log.info("EventHub producer client created successfully with proxy configuration");
            return client;
        } else {
            log.info("Configuring EventHub client without proxy");
            EventHubProducerClient client = new EventHubClientBuilder()
                    .credential(fullyQualifiedNamespace, eventhubName, tokenCredential)
                    .retry(retryOptions)
                    .buildProducerClient();
            log.info("EventHub producer client created successfully");
            return client;
        }
    }

    private static String getSASToken(String authEndpoint, String authToken, Map<String, String> properties)
            throws ConnectionRecoverableException, ConnectionUnrecoverableException {
        return AuthClient.getSASToken(authEndpoint, authToken, properties);
    }

    /**
     * Extracts the resource URI from the SAS Token.
     *
     * @param sasToken SAS token of the user
     * @return decoded resource URI from the token
     */
    private static String getResourceURI(String sasToken) {
        log.debug("Extracting resource URI from SAS token");
        String[] sasAttributes = sasToken.split("&");
        String[] resource = sasAttributes[0].split("=");
        String resourceURI = "";
        try {
            resourceURI = URLDecoder.decode(resource[1], "UTF-8");
        } catch (UnsupportedEncodingException e) {
            //never happens
        }
        //remove protocol append
        String cleanResourceURI = resourceURI.replace("sb://", "");
        if (log.isDebugEnabled()) {
            log.debug("Resource URI extracted: " + cleanResourceURI);
        }
        return cleanResourceURI;
    }

    private static String getNamespace(String resourceURI) {
        return resourceURI.split("/")[0];
    }

    private static String getEventHubName(String resourceURI) {
        return resourceURI.split("/", 2)[1];
    }
}
