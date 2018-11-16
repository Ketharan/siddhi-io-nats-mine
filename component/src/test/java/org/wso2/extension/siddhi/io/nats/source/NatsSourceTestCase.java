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

import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class NatsSourceTestCase {
    @Test
    public void testNatsTopicSource1() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name(\"SiddhiApp\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='testet', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "client.id='stan_client', "
                + "cluster.id='test-cluster'"
                + ")" +
                "define stream inputStream (name string, age int, country string);";

        String query = ("@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;");


        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);


        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //System.out.println("hi" + events);
                EventPrinter.print(events);
                for (Event event : events) {
                    System.out.println(event.toString());
                }
            }
        });

        executionPlanRuntime.start();


        Thread.sleep(60000);
    }
}


