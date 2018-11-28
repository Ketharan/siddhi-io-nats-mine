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
import io.nats.streaming.StreamingConnectionFactory;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.nats.util.NatsConstants;
import org.wso2.extension.siddhi.io.nats.util.NatsUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * Stan output transport(Handle the publishing process)  class.
 */
@Extension(
        name = "nats",
        namespace = "sink",
        description = "Nats Sink allows users to subscribe to a Stan broker and publish messages.",
        parameters = {
                @Parameter(name = NatsConstants.DESTINATION,
                        description = "Subject name which nats sink should publish to",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(name = NatsConstants.BOOTSTRAP_SERVERS,
                        description = "The nats based url of the nats server. Coma separated url values can be used "
                                + "in case of a cluster used.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NatsConstants.DEFAULT_SERVER_URL
                ),
                @Parameter(name = NatsConstants.CLIENT_ID,
                        description = "The identifier of the client publishing/connecting to the nats broker",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NatsConstants.CLUSTER_ID,
                        description = "The identifier of the nats server/cluster.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NatsConstants.DEFAULT_CLUSTER_ID
                ),
        },
        examples = {
                @Example(description = "This example shows how to publish to a nats subject with all supporting " +
                        "configurations.",
                        syntax = "@sink(type='nats', @map(type='xml'), "
                                + "destination='SP_NATS_OUTPUT_TEST', "
                                + "bootstrap.servers='nats://localhost:4222',"
                                + "client.id='nats_client',"
                                + "server.id='test-cluster',"
                                + ")\n"
                                + "define stream outputStream (name string, age int, country string);"),

                @Example(description = "This example shows how to publish to a nats subject with mandatory " +
                        "configurations.",
                        syntax = "@sink(type='nats', @map(type='xml'), "
                                + "destination='SP_NATS_OUTPUT_TEST', "
                                + ")\n"
                                + "define stream outputStream (name string, age int, country string);")
        }
)

public class NatsSink extends Sink {
    private static final Logger log = Logger.getLogger(NatsSink.class);
    private StreamingConnection streamingConnection;
    private OptionHolder optionHolder;
    private StreamDefinition streamDefinition;
    private Option destination;
    private String clusterId;
    private String clientId;
    private String natsUrl;
    private ExecutorService executorService;

    /**
     * Returns the list of classes which this sink can consume.
     * @return array of supported classes.
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
            return new String[]{NatsConstants.DESTINATION};
    }

    /**
     * Validate and initiates the nats properties and required passed parameters.
     * @param streamDefinition  containing stream definition bind to the {@link Sink}
     * @param optionHolder            Option holder containing static and dynamic configuration related
     *                                to the {@link Sink}
     * @param configReader        to read the sink related system configuration.
     * @param siddhiAppContext        the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to
     *                                get siddhi related utility functions.
     */
    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
            SiddhiAppContext siddhiAppContext) {
        this.optionHolder = optionHolder;
        this.streamDefinition = streamDefinition;
        this.executorService = siddhiAppContext.getExecutorService();
        validateAndInitStanProperties();
    }

    /**
     * Publish the given event to the nats server.
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) {
        executorService.execute(new NatsPublisher(destination.getValue(dynamicOptions), streamingConnection, payload));
    }

    /**
     * Initializes the connection to the nats server.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(this.clusterId,
                this.clientId);
        streamingConnectionFactory.setNatsUrl(this.natsUrl);

        try {

            log.info(new Date().getTime() + " nats stream connection---------------");
            streamingConnection =  streamingConnectionFactory.createConnection();
            log.info(new Date().getTime() + " nats stream connection done-------------------");
        } catch (IOException e) {
            log.error("Error while connecting to nats server at destination: " + destination);
            throw new ConnectionUnavailableException("Error while connecting to nats server at destination: "
                    + destination, e);
        } catch (InterruptedException e) {
            log.error("Error while connecting to nats server at destination: " + destination + ".The calling thread "
                    + "is interrupted before the connection can be established.");
            throw new ConnectionUnavailableException("Error while connecting to nats server at destination: "
                    + destination + " .The calling thread is interrupted before the connection can be established.", e);
        }
    }

    /**
     * Closes the {@link #streamingConnection} after usage or connection failed.
     */
    @Override
    public void disconnect() {
        try {
            if (streamingConnection != null) {
                streamingConnection.close();
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver", e);
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public Map<String, Object> currentState() {
            return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }

    private void validateAndInitStanProperties() {
        this.destination = optionHolder.validateAndGetOption(NatsConstants.DESTINATION);
        this.clusterId = optionHolder.validateAndGetStaticValue(NatsConstants.CLUSTER_ID, NatsConstants
                .DEFAULT_CLUSTER_ID);
        this.clientId = optionHolder.validateAndGetStaticValue(NatsConstants.CLIENT_ID, NatsUtils.createClientId());
        this.natsUrl = optionHolder.validateAndGetStaticValue(NatsConstants.BOOTSTRAP_SERVERS,
                NatsConstants.DEFAULT_SERVER_URL);

        NatsUtils.validateNatsUrl(natsUrl, streamDefinition.getId());
    }
}

