/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.extension.siddhi.io.nats.utils;

import io.nats.streaming.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class NatsClient {
    private String cluserId;
    private String clientId;
    private String natsUrl;
    private ResultContainer resultContainer;
    private StreamingConnectionFactory streamingConnectionFactory;
    private StreamingConnection streamingConnection;
    private Subscription subscription;
    private final CountDownLatch doneSignal = new CountDownLatch(1);
    private static Log log = LogFactory.getLog(NatsClient.class);

    public NatsClient(String clusterId, String clientId, String natsUrl, ResultContainer resultContainer) {
        this.cluserId = clusterId;
        this.clientId = clientId;
        this.natsUrl = natsUrl;
        this.resultContainer = resultContainer;
    }

    public NatsClient(String clusterId, String clientId, String natsUrl) {
        this.cluserId = clusterId;
        this.clientId = clientId;
        this.natsUrl = natsUrl;
    }

    public void connect() {
        streamingConnectionFactory = new StreamingConnectionFactory(this.cluserId,this.clientId);
        streamingConnectionFactory.setNatsUrl(this.natsUrl);
        try {
            streamingConnection =  streamingConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void publish(String subjectName, String message){
        try {
            streamingConnection.publish(subjectName,message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void subsripeFromNow(String subject) throws InterruptedException, TimeoutException, IOException {
        subscription = streamingConnection.subscribe(subject, new MessageHandler() {
            public void onMessage(Message m) {
                resultContainer.eventReceived(new String(m.getData()));
            }
        }, new SubscriptionOptions.Builder().startAtTime(Instant.now()).build());
    }

    public void subscribe(String subject) throws InterruptedException, IOException, TimeoutException {
        subscription =  streamingConnection.subscribe( subject, new MessageHandler() {
            public void onMessage(Message m) {
                System.out.printf("Client : " +  clientId +  " Received a message: %s\n", new
                        String(m.getData()));
                doneSignal.countDown();
            }
        }, new SubscriptionOptions.Builder().deliverAllAvailable().build());

        doneSignal.await();
    }

    public void subscribeFromLastPublished(String subject) throws InterruptedException, IOException, TimeoutException{
        subscription = streamingConnection.subscribe(subject, new MessageHandler() {
            public void onMessage(Message m) {
                System.out.printf("Client : " +  clientId +  " Received a message: %s\n", new
                        String(m.getData()));
                doneSignal.countDown();
            }
        }, new SubscriptionOptions.Builder().startWithLastReceived().build());
    }

    public void subscribeFromGivenSequence(String subject, int sequence) throws InterruptedException, IOException,
            TimeoutException {
        subscription = streamingConnection.subscribe(subject, new MessageHandler() {
            public void onMessage(Message m) {
                System.out.printf("Client : " +  clientId +  " Received a message: %s\n", new
                        String(m.getData()));
                doneSignal.countDown();
            }
        }, new SubscriptionOptions.Builder().startAtSequence(sequence).build());
    }

    public void subscrbeFromGivenTime(String subject,  Instant instant) throws InterruptedException, IOException,
            TimeoutException {
        subscription = streamingConnection.subscribe(subject, new MessageHandler() {
            public void onMessage(Message m) {
                System.out.printf("Client : " +  clientId +  " Received a message: %s\n", new
                        String(m.getData()));
                doneSignal.countDown();
            }
        }, new SubscriptionOptions.Builder().startAtTime(instant).build());

    }

    public void subscribeDurable(String subject, String durableName) throws InterruptedException, IOException,
            TimeoutException {
        subscription = streamingConnection.subscribe(subject, new MessageHandler() {
            public void onMessage(Message m) {
                System.out.printf("Client : " +  clientId +  " Received a message: %s\n", new
                        String(m.getData()));
                doneSignal.countDown();
            }
        }, new SubscriptionOptions.Builder().durableName(durableName).build());
    }

    public void unsubscribe() throws IOException {
        subscription.unsubscribe();
    }

    public void subscribeWithQueueGroupFromSequence(String subject, String queueGroup, int sequence)
            throws InterruptedException, TimeoutException, IOException {
        subscription = streamingConnection.subscribe(subject, queueGroup, new MessageHandler() {
            public void onMessage(Message m) {
                System.out.printf("Client : " +  clientId +  " Received a message: %s\n", new
                        String(m.getData()));
                doneSignal.countDown();
            }
        }, new SubscriptionOptions.Builder().startAtSequence(sequence).build());
    }
}
