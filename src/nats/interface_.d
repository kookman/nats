module nats.interface_;

public import core.time: Duration, seconds;

struct Msg
{
    const(char)[]  subject;
    const(char)[]  replySubject;
    uint           sid;
    uint           length;
    Subscription   subs;
    const(ubyte)[] payload;
    NatsResponse   type;

    scope string payloadAsString() @trusted
    {
        import std.string: assumeUTF;
        import std.exception: assumeUnique;
        
        return payload.assumeUTF.assumeUnique;
    }
}

/*
NatsHandlers run in the eventloop thread and block the listener task while they process
(the message stream is ordered). This leads to two constraints:
  1. Msg is scope parameter to avoid unnecessary copying/allocation
  2. Delegate must be nothrow to avoid killing the listener task with unhandled exception

If you need to do more processing, it is better to copy the information you need from the
Msg struct and send to a task or worker task (ie in a different thread)
*/ 
alias NatsHandler = void delegate(scope Msg) @safe nothrow;

class Subscription
{
    string 		subject;
    string 		queueGroup;
    ulong  		msgsReceived;
    ulong		msgsToExpire = ulong.max;
    ulong  		bytesReceived;
    NatsHandler handler;
    uint   		sid;
    bool		closed;
}

class NatsProtocolException : Exception {
    this(string message, string file = __FILE__, size_t line = __LINE__, Exception next = null) @safe
    {
        super(message, file, line, next);
    }
}

struct NatsClientConfig
{
    string      natsUri;
    string      clientId;
    Duration    heartbeatInterval = 5.seconds;
    Duration    reconnectInterval = 15.seconds;
    Duration    connectTimeout = 15.seconds;
}

enum NatsState : byte { INIT, CONNECTING, CONNECTED, DISCONNECTED, RECONNECTING, CLOSED }

enum NatsResponse : byte { FRAGMENT, MSG, MSG_REPLY, PING, PONG, INFO, OK, ERR }
