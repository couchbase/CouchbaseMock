# CouchbaseMock - The Couchbase Test Server

CouchbaseMock is a test server implementing some of the memcached protocol
which is used by some SDKs (including the C SDK) for basic testing. While it
is recommended that testing be done against the real server, CouchbaseMock is
useful as being self contained (there is no need to install it to the system)
and for allowing extra instrumentation.

CouchbaseMock is implemented in Java and is intended to be used by a single
client instance. Testing with any real kind of workload has not been done,
and it is not intended to be high performance or reliable (it does not even
persist data to the disk). As opposed to [cbgb|http://cbgb.io], this is
not intended to be a full implementation and/or replacement for the server.

The repository for CouchbaseMock may be found at
[https://github.com/couchbase/couchbasemock](https://github.com/couchbase/couchbasemock).
This is a maven project and most of us use NetBeans with it.

## Basic Usage

Typically the mock is spawned by passing a <_\--port_> argument as the REST port to
listen on, and a list of _bucket specifications_ separated by commas. Passing
_\--help_ to the CouchbaseMock should show example usage.

Once spawned, it may be used like a normal Couchbase Server. The following commands
are currently implemented

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
* GET_REPLICA (From 0.6)
* STATS
* VERSION
* VERBOSITY

A _views_ implementation is in progress.

## Out-of-band Commands

The _Out-Of-Band (OOB or Control)_ commands are where "special" commands can be
sent to the mock to do certain things which can simulate different conditions.

OOBs are sent over the _Harakiri Port._ The _Harakiri Port_ is a _client_\-side
listening port to which the mock will connect to once started up, and once the
client close the connection the mock server will perform a harakiri.
The normal "handshake" sequence is as follows:

Note that this can be found in [_tests/server.c_](https://github.com/couchbase/libcouchbase/blob/master/tests/server.c) in the libcouchbase distribution

1. The client sets up a listening address (typically on a random port \-\- i.e. passing 0 for _sin_port)._
2. Call the usual functions, i.e. _socket()_, _bind()_, and _listen()_. Then call _getsockname()_ to get the newly assigned port number
3. Invoke the CouchbaseMock JAR passing the newly assigned listening port as the argument to the _\--harakiri-monitor_ option, so e.g. _\--harakiri-monitor=localhost:42464_
4. Additionally, pass _\--port=0_ to the JAR so that it will generate a random REST port (this way we don't have port conflicts)
5. In the client, call _accept()_ on the harakiri port. The mock will connect to it.
6. Read from the new connection until an ASCII NUL is encountered. The data read will be a C string containing the ASCII representation of the newly assigned REST port.
7. Once the REST port has been received, you can use it in normal Couchbase/lcb_t operation to connect to the mock cluster.
8. Send/Receive additional OOB commands on the new \_harakiri _connection established between client and mock

## Command Format

The new command format consists of JSON objects delimited by newlines.
The JSON object will consist of the following keys:

* _command_: this is the name of the command
* _payload:_ This is an object which contains the payload for the command

The response for the command will be delivered at its most basic level will
be a JSON object consisting of the following fields

* _status_: This indicates the status of the command, it will be "ok" if the command was successful
* _payload_: (optional) - if there is more than a status to deliver

## HTTP API

This is a lightweight API following the semantics of the JSON API; only that
it uses HTTP as a transport.

The format of each command is *_[http://localhost:18091/mock/]_{*}{*}_<command>?payload_param1=payload_value1&..._*

Where <command> is the value for the JSON *command* field, and the query
parameters are expanded (URL-Encoded) fields within the *payload*.

Note that all requests (even those which modify data) use the _GET_ method;
this is to make it simple to test using a web browser.

## Command Listings

The following commands are supported by the Mock. Each command will have both
its _old-style_ (if supported) and _new-style_ arguments displayed:

### failover


This command fails over a specific server with a given index (the index is
obtained from the REST configuration). It may also be passed a bucket for
which the failover should affect (if no bucket is passed, it will be _default_)

Parameters:

<table>
<tr><th>Name</th><th>Meaning</th><th>payload-field/type</th></tr>
<tr><td>idx</td><td>The server index</td><td>_idx_, JSON Number</td></tr>
<tr><td>bucket</td><td>The bucket to affect (_default_) if unspecified</td><td>_bucket_, JSON String</td></tr>
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
<tr><th>Name</th><th>Meaning</th><th>payload-field/type</th></tr>
<tr><td>msecs</td><td>The duration of the delay in milliseconds</td><td>_msecs_, JSON Number</td></tr>
<tr><td>offset</td><td>Stall after this many bytes have been sent</td><td>_bucket_, JSON String</td></tr>
</table>

Setting both parameters to _0_ disables _hiccup_

### truncate

Chops off data from the end of each packet. As a result it means invalid data
will be sent to the client (this may also be used in conjunction with failover
to simulate a node sending partial data and then disconnecting)

Parameters:

<table>
<tr><th>Name</th><th>Meaning</th><th>payload-field/type</th></tr>
<tr><td>limit</td><td>Limit the next write operation to this many bytes</td><td>_limit_, JSON Number</td></tr>
</table>

Setting the _limit_ to _0_ disables _truncate_

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
time of writing, the Mock does not actually write anything to the dist, but
merely contains a separate storage domain for "Disk".

Thus, whenever an item is stored in the mock it may go through the following
steps:

1. The item is inserted into the vBucket master's _Cache_
2. The item is inserted into the vBucket maseter's _Disk_
3. For each vBucket replica, the item is placed inside its _Cache_
4. For each vBucket replica, the item is placed inside its _Disk_

### Common Parameters

These  out-of-band commands allow to modify or retrieve
information on a specific _key_.

They all accept a set of common parameters

<table>
    <tr>
        <th>Name</th>
        <th>Meaning</th>
        <th>Type</th>
    </tr>
    <tr>
        <td>Key</td>
        <td>The key to access</td>
        <td>Required. String</td>
    </tr>
    <tr>
        <td>OnMaster</td>
        <td>Whether to affect the key on the vBucket master</td>
        <td>Boolean. Required</td>
    <tr>
    <td>
        <td>OnReplicas</td>
        <td>Which replicas should be affected</td>
        <td>Required. This can either be a number indicating <i>how many</i>
            replicas to affect; or it can be a list of specific vBucket indices
            to affect</td>
    </tr>
    <tr>
        <td>CAS</td>
        <td>The new CAS to use</td>
        <td>Optional. Number. If not specified, the existing CAS (if the key
            already exists) of each key entry in its respective storage partition
            will be used. Otherwise a new CAS is generated</td>
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

#### keyinfo

This special command returns a JSON object containing the per-node status
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
