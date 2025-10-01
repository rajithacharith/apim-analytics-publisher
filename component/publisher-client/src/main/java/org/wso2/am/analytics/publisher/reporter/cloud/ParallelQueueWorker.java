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
package org.wso2.am.analytics.publisher.reporter.cloud;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.am.analytics.publisher.client.EventHubClient;
import org.wso2.am.analytics.publisher.exception.MetricReportingException;
import org.wso2.am.analytics.publisher.reporter.MetricEventBuilder;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Will removes the events from queues and send then to the endpoints.
 */
public class ParallelQueueWorker implements Runnable {

    private static final Logger log = LogManager.getLogger(ParallelQueueWorker.class);
    private BlockingQueue<MetricEventBuilder> eventQueue;
    private EventHubClient client;

    public ParallelQueueWorker(BlockingQueue<MetricEventBuilder> queue, EventHubClient client) {
        this.client = client;
        this.eventQueue = queue;
    }

    public void run() {
        String workerName = Thread.currentThread().getName();
        log.info("Queue worker {} started", workerName);
        
        while (true) {
            if (log.isDebugEnabled()) {
                log.debug("Queue worker {}: {} events in queue before processing", workerName, eventQueue.size());
            }
            
            MetricEventBuilder eventBuilder;
            String event;
            try {
                eventBuilder = eventQueue.take();
                if (eventBuilder != null) {
                    Map<String, Object> eventMap = eventBuilder.build();
                    event = new Gson().toJson(eventMap);
                    client.sendEvent(event);
                    
                    if (log.isDebugEnabled()) {
                        log.debug("Queue worker {}: Event processed successfully", workerName);
                    }
                }
            } catch (MetricReportingException e) {
                log.error("Queue worker {}: Event builder instance not properly configured. Event building failed", 
                          workerName, e);
                continue;
            } catch (InterruptedException e) {
                log.info("Queue worker {} interrupted", workerName);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Queue worker {}: Analytics event sending failed. Event will be dropped", workerName, e);
            }

            if (log.isDebugEnabled()) {
                log.debug("Queue worker {}: {} events remaining in queue after processing", 
                          workerName, eventQueue.size());
            }
        }
        
        log.info("Queue worker {} stopped", workerName);
    }
}
