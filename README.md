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

This will first describe the "legacy" format - which is a comma-separated
list of strings terminated by a newline (0xa, '\n'). The first field is the
name of the command itself, and the subsequent fields are arguments for the
command. The old-style command protocol does not feature responses and can
only accept commands.

Due to timing issues it was necessary to implement a new protocol which can
send back responses - at which point a client can acknowledge that
something was "done".

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
