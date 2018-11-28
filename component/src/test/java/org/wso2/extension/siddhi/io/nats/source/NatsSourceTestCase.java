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

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.nats.utils.NatsClient;
import org.wso2.extension.siddhi.io.nats.utils.ResultContainer;
import org.wso2.extension.siddhi.io.nats.utils.UnitTestAppender;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class NatsSourceTestCase {
    private static Logger log = Logger.getLogger(NatsSourceTestCase.class);
    private String clientId;
    private AtomicInteger eventCounter = new AtomicInteger(0);

    @BeforeMethod
    private void setUp(){
        eventCounter.set(0);
    }

    /**
     * Test the ability to subscripe to a nats topic based on sequence number.
     * @throws InterruptedException
     */
    @Test
    public void testNatsSequenceSubscribtion() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NatsClient natsClient = new NatsClient("test-cluster", "nats-source-test1",
                "nats://localhost:4222");
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='nats-source-test1-siddhi', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("nats-test1","<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test1","<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * if a property missing from the siddhi stan source which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test
    public void testMissingNatsMandatoryProperty(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name(\"Test-plan2\")"
                + "@source(type='nats', @map(type='xml'), "
                + "bootstrap.servers='nats://localhost:4222', "
                + "client.id='nats-source-test2-siddhi', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        try {
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
            Assert.fail();
        } catch (SiddhiAppValidationException e) {
            Assert.assertTrue(e.getMessage().contains("'destination' 'static' option is not defined in the "
                    + "configuration of source:nats"));
        }
        siddhiManager.shutdown();
    }

    /**
     * If invalid nats url provided then {@link SiddhiAppCreationException} will be thrown
     */
    @Test
    public void testInvalidNatsUrl(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan3')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "bootstrap.servers='natss://localhost:4222', "
                + "client.id='nats-source-test1-siddhi', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        try {
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
            Assert.fail();
        } catch (SiddhiAppCreationException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid nats url"));
        }
        siddhiManager.shutdown();
    }

    /**
     * The load of a subject should be shared between clients when more than one clients subscribes with a same queue
     * group name
     */
    @Test
    public void testQueueGroupSubscription() throws InterruptedException, IOException, TimeoutException {
        clientId = "Test-Plan-4_" + new Date().getTime();
        Thread.sleep(100);
        AtomicInteger instream1Count = new AtomicInteger(0);
        AtomicInteger instream2Count = new AtomicInteger(0);
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition1 = "@App:name('Test-plan4-1')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test4', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "client.id='" + clientId +  "', "
                + "cluster.id='test-cluster',"
                + "queue.group.name = 'test-plan4'"
                + ")"
                + "define stream inputStream1 (name string, age int, country string);";

        clientId = "Test-Plan-5_" + new Date().getTime();
        String inStreamDefinition2 = "@App:name('Test-plan4-2')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test4', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "client.id='" + clientId +  "', "
                + "cluster.id='test-cluster',"
                + "queue.group.name = 'test-plan4'"
                + ")"
                + "define stream inputStream2 (name string, age int, country string);";

        clientId = "Test-Plan-4_" + new Date().getTime();
        NatsClient natsClient = new NatsClient("test-cluster", clientId,
                "nats://localhost:4222");
        natsClient.connect();
        natsClient.subsripeFromNow("nats-test4");

        SiddhiAppRuntime inStream1RT = siddhiManager.createSiddhiAppRuntime(inStreamDefinition1);
        SiddhiAppRuntime inStream2RT = siddhiManager.createSiddhiAppRuntime(inStreamDefinition2);

        inStream1RT.addCallback("inputStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    instream1Count.incrementAndGet();
                }
            }
        });

        inStream2RT.addCallback("inputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    instream2Count.incrementAndGet();
                }
            }
        });
        inStream1RT.start();
        inStream2RT.start();

        natsClient.publish("nats-test4","<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4","<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(8000);

        Assert.assertTrue(instream1Count.get() != 0, "Total events should be shared between clients");
        Assert.assertTrue(instream2Count.get() != 0, "Total events should be shared between clients");
        Assert.assertEquals(instream1Count.get() + instream2Count.get(), 10);

        siddhiManager.shutdown();
        natsClient.unsubscribe();
        natsClient.close();
    }

    /**
     * if the client.id is not given by the user in the extension headers, then a randomly generated client id will
     * be used.
     */
    @Test()
    public void testOptionalClientId() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NatsClient natsClient = new NatsClient("test-cluster", "nats-source-test-5",
                "nats://localhost:4222");
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan5\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("nats-test1","<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test1","<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * If a single stream has multiple source annotations then all the events from those subjects should be passed to
     * the stream
     */
    @Test
    public void testMultipleSourceSingleStream() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(4,3);
        NatsClient natsClient = new NatsClient("test-cluster", "nats-source-test6",
                "nats://localhost:4222");
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan6\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test6-sub1', "
                + "client.id='nats-source-test6-siddhi-1', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "cluster.id='test-cluster'"
                + ")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test6-sub2', "
                + "client.id='nats-source-test6-siddhi-2', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("nats-test6-sub1","<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test6-sub1","<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test6-sub2","<events><event><name>JHON</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test6-sub2","<events><event><name>SMITH</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("JHON"));
        Assert.assertTrue(resultContainer.assertMessageContent("SMITH"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Evaluate the subject subscription configuration with the source pause and resume.
     */
    @Test
    public void testNatsSourcePause() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NatsClient natsClient = new NatsClient("test-cluster", "nats-source-test7",
                "nats://localhost:4222");
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan7\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test7', "
                + "client.id='nats-source-test7-siddhi', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });

        Collection<List<Source>> sources = executionPlanRuntime.getSources();
        executionPlanRuntime.start();
        sources.forEach(e -> e.forEach(Source::pause));

        Thread.sleep(1000);

        natsClient.publish("nats-test7","<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        sources.forEach(e -> e.forEach(Source::resume));

        natsClient.publish("nats-test7","<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Test subscription to a nats topic based on sequence number with mandatory configurations only.
     * @throws InterruptedException
     */
    @Test
    public void testNatsSequenceSubscribtionWithMandatoryConfigs() throws InterruptedException, IOException,
            TimeoutException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NatsClient natsClient = new NatsClient("test-cluster", "nats-source-test8",
                "nats://localhost:4222");
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan8\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test8' "
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("nats-test8","<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test8","<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * If invalid cluster name is provided in nats source configurations then {@link ConnectionUnavailableException}
     * should have been thrown. Here incorrect cluster id provided hence the connection will fail.
     */
    @Test
    public void testInvalidClusterName() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan9\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test9', "
                + "client.id='nats-source-test9-siddhi', "
                + "bootstrap.servers='nats://localhost:4222', "
                + "cluster.id='nats-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.start();
        Thread.sleep(1000);

        Assert.assertTrue(appender.getMessages().contains("Error while connecting to nats server at destination: "
                + "nats-test9"));
        siddhiManager.shutdown();
    }

    /**
     * If incorrect bootstrap server url is provided in nats source configurations then
     * {@link ConnectionUnavailableException} should have been thrown. Here incorrect cluster url is provided hence the
     * connection will fail.
     */
    @Test
    public void testIncorrectNatsServerUrl() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan10\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test10', "
                + "client.id='nats-source-test10-siddhi', "
                + "bootstrap.servers='nats://localhost:5223', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.start();
        Thread.sleep(1000);

        Assert.assertTrue(appender.getMessages().contains("Error while connecting to nats server at destination: "
                + "nats-test10"));
        siddhiManager.shutdown();
    }

}


