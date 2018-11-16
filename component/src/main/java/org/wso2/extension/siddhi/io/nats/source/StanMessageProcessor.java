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
package org.wso2.extension.siddhi.io.nats.source;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Process the nats subject subscription channel in concurrent safe manner.
 */
public class StanMessageProcessor implements MessageHandler {
    private SourceEventListener sourceEventListener;
    private boolean paused;
    private ReentrantLock lock;
    private Condition condition;
    private AtomicInteger messageSequenceTracker;

    public StanMessageProcessor(SourceEventListener sourceEventListener, SiddhiAppContext siddhiAppContext ,
                                AtomicInteger messageSequenceTracker) {
        this.sourceEventListener = sourceEventListener;
        this.messageSequenceTracker = messageSequenceTracker;
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    @Override
    public void onMessage(Message msg) {
        if (paused) {
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        sourceEventListener.onEvent(msg.getData(), new String[0]);
        messageSequenceTracker.incrementAndGet();
    }

    public AtomicInteger getMessageSequenceTracker() {
        return messageSequenceTracker;
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
