/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.io.nats.sink;

import io.nats.streaming.StreamingConnection;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.nats.sink.exception.NatsSinkAdaptorRuntimeException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Handle the logic to publish messages to nats server in a concurrent manner.
 */
public class NatsPublisher implements Runnable {
    private static final Logger log = Logger.getLogger(NatsPublisher.class);
    private String subjectName;
    private StreamingConnection streamingConnection;
    private String message;

    public NatsPublisher(String subjectName, StreamingConnection streamingConnection, Object payload) {
        this.subjectName = subjectName;
        this.streamingConnection = streamingConnection;

        try {
            this.message = handleMessage(payload);
        } catch (NatsSinkAdaptorRuntimeException e) {
            throw new NatsSinkAdaptorRuntimeException("Error while processing the Stan message to destination "
                    + subjectName, e);
        }
    }

    public void run() {
        try {
            streamingConnection.publish(subjectName, message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("Error sending message to destination: " + subjectName);
            throw new NatsSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName, e);
        } catch (InterruptedException e) {
            log.error("Error sending message to destination: " + subjectName +  ".The calling thread is "
                            + "interrupted before the call completes.");
            throw new NatsSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".The calling thread is interrupted before the call completes.", e);
        } catch (TimeoutException e) {
            log.error("Error sending message to destination: " + subjectName + ".Timeout occured while trying to ack.");
            throw new NatsSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".Timeout occured while trying to ack.", e);
        }
    }

    private String handleMessage(Object payload) {
        String message;
        if (payload instanceof String) {
            return  (String) payload;
        } else {
            throw new NatsSinkAdaptorRuntimeException("The message type is not supported by nats clients");
        }
    }
}
