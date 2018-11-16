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

import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class NatsSinkTestCase {
    /**
     * Test for configure the nats Sink publish the message to an nats-streaming topic.
     */
    @Test
    public void natsPublishTest() throws InterruptedException {
        SiddhiAppRuntime executionPlanRuntime = null;
        //ResultContainer resultContainer = new ResultContainer(2);
        //JMSClient client = new JMSClient("activemq", "DAS_JMS_OUTPUT_TEST", "", resultContainer);
        SiddhiManager siddhiManager = new SiddhiManager();

        //init
        //Thread listenerThread = new Thread(client);
        //listenerThread.start();
        //Thread.sleep(1000);
        // deploying the execution plan



        String inStreamDefinition = "" +
                "@sink(type='nats', @map(type='xml'), "
                + "destination='tests', "
                + "bootstrap.servers='nats://localhost:4222',"
                + "client.id='stan_client',"
                + "cluster.id='test-cluster'"
                + ")" +
                "define stream inputStream (name string, age int, country string);";
        executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        //Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        //Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));


        //client.shutdown();


        siddhiManager.shutdown();
    }

}

