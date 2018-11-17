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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.nats.utils.NatsClient;
import org.wso2.extension.siddhi.io.nats.utils.ResultContainer;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class NatsSinkTestCase {
    /**
     * Test for configure the nats Sink to publish the message to a nats-streaming subject.
     */
    @Test
    public void natsSimplePublishTest() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NatsClient natsClient = new NatsClient("test-cluster","stan_test1","nats://localhost:4222"
                , resultContainer);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan1')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "bootstrap.servers='nats://localhost:4222',"
                + "client.id='stan_client',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        natsClient.subsripeFromNow("nats-test1");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(1000);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));

        siddhiManager.shutdown();
    }

    /**
     * if a property missing from the siddhi stan sink which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test
    public void testMissingNatsMandatoryProperty(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan2')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "bootstrap.servers='nats://localhost:4222',"
                + "client.id='test-plan2',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        try {
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
            Assert.fail();
        } catch (SiddhiAppValidationException e) {
            Assert.assertTrue(e.getMessage().contains("Option 'destination' does not exist in the configuration of "
                    + "'sink:nats'"));
        }
    }

    /**
     * If invalid nats url provided then {@link SiddhiAppCreationException} will be thrown
     */
    @Test
    public void testInvalidNatsUrl(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan3')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test3', "
                + "bootstrap.servers='natss://localhost:4222',"
                + "client.id='stan_client',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        try {
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
            Assert.fail();
        } catch (SiddhiAppCreationException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid nats url"));
        }
    }
}

