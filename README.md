# CouchbaseMock - The Couchbase Test Server

CouchbaseMock is a test server implementing some of the memcached protocol
which is used by some SDKs (including the C SDK) for basic testing. While it
is recommended that testing be done against the real server, CouchbaseMock is
useful as being self-contained (there is no need to install it to the system)
and for allowing extra instrumentation.

CouchbaseMock is implemented in Java and is intended to be used by a single
client instance. Testing with any real kind of workload has not been done,
and it is not intended to be high performance or reliable (it does not even
persist data to the disk). As opposed to [cbgb](https://github.com/couchbaselabs/cbgb), this is
not intended to be a full implementation and/or replacement for the server.

The repository for CouchbaseMock may be found at
[https://github.com/couchbase/couchbasemock](https://github.com/couchbase/couchbasemock).
This is a maven project and most of us use NetBeans with it.

## Basic Usage

Typically, the mock is spawned by passing a `--port` argument as the REST port to
listen on, and a list of _bucket specifications_ separated by commas. Passing
`--help` to the CouchbaseMock should show example usage. If no port is passed,
it will listen on the REST port of 8091 (and will fail if you already have a
Couchbase Server running).

By default, the mock will be up and running with the `default` bucket.

Once the mock has been started, it may be used like a normal Couchbase server,
with clients bootstrapping over HTTP using the port specified as `--port`.

```sh
java -jar CouchbaseMock-1.5.26.jar --port 9000
```

and then in another console:

```sh
cbc cat foo -U http://localhost:9000
```

If you are using CouchbaseMock in automated testing, the test application
should open up a control port, pass that port to the mock using the
`--harakiri-monitor` argument and set `--port` to 0. The randomly assigned
HTTP port will be received on the control port once. See the
_Out of Band Commands_ section below.

## Supported Couchbase Operations

### Memcached (Key-Value)

* GET
* GETQ
* GAT
* GATQ
* TOUCH
* SET
* APPEND
* PREPEND
* REPLACE
* ADD
* REMOVE
* INCR/DECR
* GETL (From 0.6)
* UNL (From 0.6)
* OBSERVE (From 0.6)
* GET\_REPLICA (From 0.6)
* STATS
* VERSION
* VERBOSITY

### Administrative REST API

These standard REST API endpoints are supported. See the Couchbase Administration
manual for how to use these endpoints. These behave exactly as they would
against a real Couchbase cluster.

The username and password are hard-coded into the mock as `Administrator` and
`password` respectively.

* `/pools` (GET)
* `/pools/default` (GET)
* `/pools/default/buckets` (GET, POST) - allows for bucket creation
* `/pools/default/buckets/$bucket` (GET, DELETE) - allows for bucket deletion
* `/pools/default/buckets/$bucket/ddocs` (GET) - allows for listing design documents
* `/pools/default/bucketsStreaming/$bucket` (GET) - streaming config URI
* `/sampleBuckets/install` (POST) - allows loading the `beer-sample` bucket.
  Note that this endpoint seems to be undocumented.

Note that only SASL-auth buckets may be created. This does not necessarily
mean that your bucket must have a password. For example:

    curl -XPOST -u Administrator:password \
        localhost:8091/pools/default/buckets \
        -d bucketType=couchbase \
        -d name=newbucket \
        -d authType=sasl \
        -d ramQuotaMB=200

Will create a bucket without a password.

Additionally, note that the `ramQuotaMB` must be specified, though other than
being necessary for conforming to server behavior, has no effect.

### Views (Map-Reduce)

The following rest endpoints are supported. Note that the view query port
(e.g. the _capi_ port) is the same as the administrative port. This should
not matter for conforming clients which determine this information from
the cluster configuration endpoint.

Both `map` and `reduce` functions are supported. Javascript support is provided
using Rhino, so view functions which depend on V8-specific functionality
may fail.

The `beer-sample` bucket is available in the mock and may be loaded by passing
the `-S` option on the commandline. It may also be loaded in-situ by using
the `/sampleBuckets/install` REST API, for example:

    curl -u Administrator:password localhost:8091/sampleBuckets/install \
        -X POST \
        -H "Content-Type: application/json" \
        -d '["beer-sample"]'

Accessing views may be done by the following endpoints:

* `/$bucket/_design/$ddoc` (PUT, GET, DELETE) - used to create or remove design
  documents
* `/$bucket/_design/$ddoc/_view/$view` - to query a view

The following view parameters are recognized and have effect

* skip
* limit
* reduce
* group
* group\_level
* startkey
* startkey\_docid
* endkey
* endkey\_docid
* key
* keys
* inclusive\_start (NOTE: not in Couchbase)
* inclusive\_end
* descending
* debug (returns dummy debug info)

The `full_set` and `stale` options are ignored.

## Out-of-band Commands

The _Out-Of-Band (OOB or Control)_ commands are where "special" commands can be
sent to the mock to do certain things which can simulate different conditions.

OOBs are sent over the _Harakiri Port._ The _Harakiri Port_ is a _client_\-side
listening port to which the mock will connect to once started up, and once the
client close the connection the mock server will perform a harakiri.
The normal "handshake" sequence is as follows:

Note that this can be found in [_tests/server.c_](https://github.com/couchbase/libcouchbase/blob/master/tests/server.c) in the libcouchbase distribution

1. The client sets up a listening address (typically on a random port
-- i.e. passing 0 for `sin_port`).
2. Call the usual functions, i.e. `socket()`, `bind()`, and `listen()`.
Then call `getsockname()` to get the newly assigned port number
3. Invoke the CouchbaseMock JAR passing the newly assigned listening port as the argument to the `--harakiri-monitor` option, so e.g. `--harakiri-monitor=localhost:42464`
4. Additionally, pass `--port=0` to the JAR so that it will generate a random REST port (this way we don't have port conflicts)
5. In the client, call _accept()_ on the harakiri port. The mock will connect to it.
6. Read from the new connection until an ASCII NUL is encountered. The data read will be a C string containing the ASCII representation of the newly assigned REST port.
7. Once the REST port has been received, you can use it in normal Couchbase/lcb\_t operation to connect to the mock cluster.
8. Send/Receive additional OOB commands on the new _harakiri_ connection established between client and mock

## Command Format

The command format consists of JSON objects delimited by newlines.
The JSON object will consist of the following keys.

* _command_: this is the name of the command
* _payload:_ This is an object which contains the payload for the command

The response for the command will be delivered at its most basic level will
be a JSON object consisting of the following fields

* _status_: This indicates the status of the command, it will be "ok" if the command was successful
* _payload_: (optional) - if there is more than a status to deliver

## HTTP API

This is a lightweight API following the semantics of the JSON API; only that
it uses HTTP as the transport.

The format of each command is `http://localhost:18091/mock/<command>?payload\_param1=payload\_value1&...`

Where <command> is the value for the JSON *command* field, and the query
parameters are expanded (URL-Encoded) fields within the *payload*.

Note that all requests (even those which modify data) use the _GET_ method;
this is to make it simple to test using a web browser.

## Command Listings

The following commands are supported by the Mock.
The payload for each command should contain dictionary keys corresponding
to the listed _Name_ of the parameter, and its value should conform to the
specified _Type_.

### failover


This command fails over a specific server with a given index (the index is
obtained from the REST configuration). It may also be passed a bucket for
which the failover should affect (if no bucket is passed, it will be _default_).
Names in *bold* are *required*

Parameters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td><b>idx</b></td>
        <td>The server index</td>
        <td>JSON Number</td>
    </tr>
        <tr><td>bucket</td>
        <td>The bucket to affect (`"default"`) if unspecified</td>
        <td>JSON String</td>
    </tr>
</table>

### respawn

This command does the opposite of _failover_. Call this with the same arguments
as _failover_ to re-activate the node which was failed over.

### hiccup

Schedules an artificial delay after a _memcached_ server has sent a
specific amount of data. This is intended to simulate a scenario where a
server hangs or stalls after sending out a partial packet.

Parameters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>

    <tr>
        <td><b>msecs</b></td>
        <td>The duration of the delay in milliseconds</td>
        <td>JSON Number</td>
    </tr>
    <tr>
        <td><b>offset</b></td>
        <td>Stall after this many bytes have been sent</td>
        <td>JSON Number</td>
    </tr>
</table>

Setting both parameters to _0_ disables _hiccup_

### truncate

Chops off data from the end of each packet. As a result it means invalid data
will be sent to the client (this may also be used in conjunction with failover
to simulate a node sending partial data and then disconnecting)

Parameters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td><b>limit</b></td>
        <td>Limit the next write operation to this many bytes</td>
        <td>JSON Number</td>
    </tr>
</table>

Setting the _limit_ to _0_ disables _truncate_

### OpFail

Causes a number of memcached operations to unconditionally fail with a
specific error code. This may be used to simulate simple 'OOM' or
`NOT_MY_VBUCKET` errors.

Paramters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td><b>code</b></td>
        <td>The Memcached protocol code to force</td>
        <td>JSON Number; Must also be recognized by the Mock</td>
    </tr>
    <tr>
        <td><b>count</b></td>
        <td>The number of times this error code should be sent
            before normal operation is restored. This can be either
            a positive number (which indicates that this many operations
            should fail before restoring to normal operation), 0 (which
            means that normal behavior be restored immediately) or a
            negative number, in which case commands will fail indefinitely
            until a 0 is sent again with this command</td>
        <td>JSON Number</td>
    </tr>
    <tr>
        <td>servers</td>
        <td>A list of servers to apply this setting to. Servers are specified
        as indices into the server array. By default, all servers are used</td>
        <td>JSON Number</td>
    </tr>
</table>

### Time Travel

This command moves the internal clock in the server. The primary purpose
for this is to allow the clients to test TTL without having to "sleep".
Names in *bold* are *required*

Parameters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td><b>Offset</b></td>
        <td>The number of seconds to add to the internal clock</td>
        <td>JSON Number</td>
    </tr>
</table>


### SET_CCCP

This command enables or disables *CCCP* protocol semantics for a group of
servers.


Parameters:

<table>
    <tr>
       <th>Name</th>
       <th>Meaning</th>
       <th>Type</th>
    </tr>
    <tr>
        <td><b>enabled</b></td>
        <td>Whether to enable or disabled CCCP on the selection criteria</td>
        <td>JSON Boolean</td>
    </tr>
    <tr>
        <td>bucket</td>
        <td>Bucket for which CCCP should be enabled/disabled.
        If this is empty, then this command affects all buckets</td>
        <td>String</td>
    </tr>
    <tr>
        <td>servers</td>
        <td>An array of server indices for which the enable/disable setting should
        apply to. If this is not set, then all servers are modified</td>
        <td>Array of numbers</td>
    </tr>
 </table>

### SET_ENHANCED_ERRORS

This command enables or disables *Enhanced Error Messages* semantics for a group of
servers. See [SDK-RFC-28](https://github.com/couchbaselabs/sdk-rfcs) for details.

Parameters:

<table>
    <tr>
       <th>Name</th>
       <th>Meaning</th>
       <th>Type</th>
    </tr>
    <tr>
        <td><b>enabled</b></td>
        <td>Whether to enable or disabled Enhanced Errors on the selection criteria</td>
        <td>JSON Boolean</td>
    </tr>
    <tr>
        <td>bucket</td>
        <td>Bucket for which Enhanced Errors should be enabled/disabled.
        If this is empty, then this command affects all buckets</td>
        <td>String</td>
    </tr>
    <tr>
        <td>servers</td>
        <td>An array of server indices for which the enable/disable setting should
        apply to. If this is not set, then all servers are modified</td>
        <td>Array of numbers</td>
    </tr>
</table>

### SET_COMPRESSION

This command sets compression mode for the group of servers. See [SDK-RFC-30](https://github.com/couchbaselabs/sdk-rfcs) for details.

Parameters:

<table>
    <tr>
       <th>Name</th>
       <th>Meaning</th>
       <th>Type</th>
    </tr>
    <tr>
        <td><b>mode</b></td>
        <td>Operating mode for the server: <code>"off"</code>, <code>"passive"</code>, <code>"active"</code> (see spec for details).
        It also has extra mode <code>"disabled"</code>, which makes mock to behave like it does not know Snappy at all.</td>
        <td>String</td>
    </tr>
    <tr>
        <td>bucket</td>
        <td>Bucket for which Enhanced Errors should be enabled/disabled.
        If this is empty, then this command affects all buckets</td>
        <td>String</td>
    </tr>
    <tr>
        <td>servers</td>
        <td>An array of server indices for which the enable/disable setting should
        apply to. If this is not set, then all servers are modified</td>
        <td>Array of numbers</td>
    </tr>
</table>

### GET_MCPORTS

This is a more convenient way to get the memcached ports without parsing the
entire vBucket config. This is particularly useful for `libcouchbase`' tests
which at the time of writing don't have access to a simple HTTP implementation

Parameters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td>bucket</td>
        <td>Which bucket to use. If unspecified, <i>default</i> is used</td>
        <td>string</td>
    </tr>
</table>

The response shall contain in the `payload` field a JSON array of integers
containing port numbers (relative to the Mock's listening addresses) which may
be used as memcached ports.

### keyinfo

This command returns the information about a given key in the mock
server. Names in *bold* are *required*

Parameters:

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td><b>key</b></td>
        <td>The key to access</td>
        <td>JSON String</td>
    </tr>
    <tr>
        <td>Bucket</td>
        <td>The bucket in which the key resides</td>
        <td>Optional. String. If not specified, <pre>"default"</pre> is used</td>
    </tr>
</table>


The payload contains a JSON object containing the per-node status
of a given key. The base object is a JSON array (`[]`). Each element in
the array is a JSON object containing three fields.

The nodes are ordered according to the server list received in the vBucket
configuration.

If the server is neither a replica nor a master for the given key, it is
present as `null`.

* Conf:
    Configuration information about this node in relation to the keys' vBucket.
    This is a JSON object containing two subfields:

    * Index - the server index in the vBucket map for the given vBucket. If this is
    a master, the index will be `0`
    * Type - Either `master` or `replica`

* Cache:
    This is a JSON object containing the status of the key as it resides in the
    node's _Cache_. If the item is not present within the node's cache, the
    object is empty; otherwise it contains these subfields:

    * CAS The CAS value of the key as present within the storage domain
    * Value the actual value of the key

* Disk:
    This carries the same semantics as `Cache`, only that it displays information
    relating to the node's _Disk_ storage domain.


## Key Access Commands

### Concepts

Starting with 0.6, the Mock introduces actual storage layers to emulate those of
an actual cluster. Specifically, a cluster has one or more nodes, where each node
retains a key in both its volatile memory (_Cache_) and persistent storage
(_Disk_). While from a user perspective this process tends to be transparent, the
distinction makes itself known when operating on things such as views (where
indices are built from "persisted" items only) and the various `OBSERVE` and/or
durability/persistence-requirement commands as well as get-from-replica.

Note that _Cache_ and _Disk_ represent abstract concepts in the Mock. At the
time of writing, the Mock does not actually write anything to the disk, but
merely contains a separate storage domain for "Disk".

Thus, whenever an item is stored in the mock it may go through the following
steps:

1. The item is inserted into the vBucket master's _Cache_
2. The item is inserted into the vBucket master's _Disk_
3. For each vBucket replica, the item is placed inside its _Cache_
4. For each vBucket replica, the item is placed inside its _Disk_

### Common Parameters

These out-of-band commands allow to modify or retrieve
information on a specific _key_.

They all accept a set of common parameters

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td><b>Key</b></td>
        <td>The key to access</td>
        <td>JSON String</td>
    </tr>
    <tr>
        <td><b>OnMaster</b></td>
        <td>Whether to affect the key on the vBucket master</td>
        <td>JSON Boolean</td>
    </tr>
    <tr>
        <td><b>OnReplicas</b></td>
        <td>Which replicas should be affected</td>
        <td>This can either be a number indicating <i>how many</i>
            replicas to affect; or it can be a list of specific replica indices
            to affect</td>
    </tr>
    <tr>
        <td>CAS</td>
        <td>The new CAS to use</td>
        <td>Optional. Number. If not specified, the existing CAS (if the key
            already exists) of each key entry in its respective storage partition
            will be used. Otherwise, a new CAS is generated</td>
    </tr>
    <tr>
        <td>Value</td>
        <td>The new value to use</td>
        <td>Optional. String. If not specified the items value will be
            an empty string</td>
    </tr>
    <tr>
        <td>Bucket</td>
        <td>The bucket in which the key resides</td>
        <td>Optional. String. If not specified, <pre>"default"</pre> is used</td>
    </tr>
</table>


Below is a list of commands which accept these common parameters


### Commands


#### persist

This command will store an item to one or more nodes' _Disk_ storage domain.
The nodes affected depend on the `OnMaster` and `OnReplicas` parameters

#### unpersist

Remove an item from the _Disk_ storage domain from the selected nodes

#### cache

Store an item to one or more nodes' _Cache_

#### uncache

Remove an item from one or more nodes' _Cache_

#### endure

For each affected node, store the item to its _Disk_ *and* _Cache_ stores.
This is equivalent to calling `persist` and `cache` on the same item

#### purge

For each affected node, remove the item from both its _Disk_ and _Cache_
stores. This is equivalent to calling `uncache` and `unpersist` on the same
item

## Retry Verification Commands

The mock can verify whether a client library is properly retrying commands.
To use this feature, your client must have the `XERROR` feature enabled and
implemented.

The usage overview is this:

1. Setup retry verification recording before running your test
2. Instruct the mock to inject failures (using the `OPFAIL` command), which
   will trigger the retry. Note that you can inject failures using other means
   as well (e.g. internally at the client level)
3. Execute your operations with failure injection enabled (per step 2)
4. Once the operations are done, request the mock to verify behavior. This will
   also implicitly undo whatever setup was done in step 1.

You can set up retry verification using the `START_RETRY_VERIFY` command. This command requires the server index and the bucket name to use for verification.

Because the precise server index must be known, you will need to determine the vBucket
for the key that will be used in the commands.

```python
# Pseudo code
test_key = "HelloWorld"
vbucket = vbucket_map(test_key)
server = server_map(vbucket)
```

In order to trigger the correct error retry behavior, you will need to use special error return codes which are further defined in the Mock itself. If the client is
properly implemented and functioning correctly, it should not have explicit
knowledge of these error codes - but should be handled dynamically by using the
corresponding entry in the error map.

The error codes are:

* `0x7ff0` - defines a *constant* retry strategy
* `0x7ff1` - defines a *linear* retry strategy
* `0x7ff2` - defines an *exponential* retry strategy.

You should test with all three error codes to verify that the client can correctly
handle all three strategies.

Then set up the `OPFAIL` command to be directed towards the correct server:

```python
cmd = {
    'command': 'OPFAIL',
    'payload': {
        'servers': [server],
        'bucket': 'default',
        'count': 100,
        'code': 0x7ff1
    }
}
```

And then set up the `START_RETRY_VERIFY` command.

```python
cmd = {
    'command': 'START_RETRY_VERIFY',
    'payload': {
        'idx': server,  # Numeric index of server
        'bucket': 'default'
    }
}
```

Note that `START_RETRY_VERIFY` only tells the server to start recording command logs.

Once the `OPFAIL` is sent, any command directed to the specified servers will cause
the server to return the error code specified. For the purposes of testing, a simple
`GET` (`0x00`) command should suffice to trigger the error behavior.

The command should eventually fail on the client side - after multiple internal retries by the client.

Once the client has failed the command, you should query the mock to determine if
the client had behaved as expected. This is done using the `CHECK_RETRY_VERIFY` command.

The `CHECK_RETRY_VERIFY` command requires the server index (from above), the error, and the opcode of the test operation. If verification succeeded, the command will
return success, and if it failed, it will return an error with some additional
information.

The format of `CHECK_RETRY_VERIFY` is:

```python
'command': {
    'payload': {
        'idx': server,
        'bucket': 'default',
        'errcode': 0x7ff1,
        'opcode': 0x00
    }
}
```

The mock uses three basic tests to determine if the retries were done according
to spec.

1. The first retry should be within `after` (+/- fuzz) ms of the first
   error response.
2. The spacing between each interval should be as prescribed (strategy dependent)
3. The *last* retry should not exceed past (+/- fuzz) ms of the
   `max-duration` interval from the receipt of the first error.

The "fuzz" is defined as 10ms. Fuzz is required because not all operating
environments (virtual machines, interpreters, clocks, etc.) can guarantee
accuracy/resolution within 1 ms.


In the following example, conformance to the `constant` strategy is
verified. We will assume that the clock begins at 0 ms


Here is the spec:

```
{
    "strategy": "constant",
    "max-duration": 500,
    "interval": 10,
    "after": 50
}
```

* 0ms: Client receives error response
* 53ms: After waiting ~50ms, the client sends the first retry. This should
  be within fuzz+`after` of first error response.
* 60ms: After waiting ~10ms, the client sends the next retry.
  This should be within fuzz+`interval` of the previous retry attempt.
* 69ms: Retry
* 75ms: Retry
....
* 484ms: Client sends retry...
* 497ms: Client sends last retry. Should be within interval+fuzz of
  +500ms (Time since first error response)


Verifying this with linear or exponential retries is similar. Here's an example
for linear retry:

```
{
    "strategy": "linear",
    "max-duration": 500,
    "interval": 10,
    "after": 50,
    "ceil": 200
}
```

* 0ms: Client receives error response
* 55ms: Client sends first retry
* 62ms: Client sends next retry (interval is 10ms)
* 87ms: Client sends next retry (interval is now 20ms)
* 109ms: Client sends next retry (interval is now 30ms)
* 142ms: Client sends next retry (interval is now 40ms)
* 195ms: Client sends next retry (interval is now 50ms)
* 256ms: Client sends next retry (interval is now 60ms)
...
