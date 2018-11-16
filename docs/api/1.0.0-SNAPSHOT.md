# API Docs - v1.0.0-SNAPSHOT

## Sink

### nats *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*

<p style="word-wrap: break-word">Stan Sink allows users to subscribe to a Stan broker and publish messages.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="nats", destination="<STRING>", bootstrap.servers="<STRING>", client.id="<STRING>", cluster.id="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">destination</td>
        <td style="vertical-align: top; word-wrap: break-word">Subject name which Stan sink should publish to</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word">The nats based url of the nats server. Coma separated url values can be used in case of a cluster used.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">client.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The identifier of the client publishing/connecting to the nats broker</td>
        <td style="vertical-align: top">stan_client</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">cluster.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The identifier of the nats server/cluster.</td>
        <td style="vertical-align: top">test-cluster</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='nats', @map(type='xml'), destination='SP_STAN_OUTPUT_TEST', bootstrap.servers='nats://localhost',client.id='stan_client'server.id='test-cluster)
define stream outputStream (name string, age int, country string);
```
<p style="word-wrap: break-word">This example shows how to publish to a nats subject.</p>

## Source

### nats *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">Stan Source allows users to subscribe to a Stan broker and receive messages. It has the ability to receive Text based messages.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="nats", destination="<STRING>", bootstrap.servers="<STRING>", client.id="<STRING>", cluster.id="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">destination</td>
        <td style="vertical-align: top; word-wrap: break-word">Subject name which Stan Source should subscribe to</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word">The nats based url of the nats server. Coma seperated url values can be used in case of a cluster used.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">client.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The identifier of the client subscribing/connecting to the nats broker</td>
        <td style="vertical-align: top">stan_client</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">cluster.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The identifier of the nats server/cluster.</td>
        <td style="vertical-align: top">test-cluster</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='nats', @map(type='text'), destination='SP_STAN_INPUT_TEST', bootstrap.servers='nats://localhost:4222',client.id='stan_client'server.id='test-cluster)
define stream inputStream (name string, age int, country string);
```
<p style="word-wrap: break-word">This example shows how to subscribe to a nats subject.</p>

