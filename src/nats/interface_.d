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

    import std.string: assumeUTF;
    string payloadAsString() @safe
    {
        return assumeUTF(payload);
    }
}

alias NatsHandler = void delegate(scope Msg) @safe;

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

enum NatsState : byte { CONNECTING, CONNECTED, DISCONNECTED, RECONNECTING, CLOSED }

enum NatsResponse : byte { FRAGMENT, MSG, MSG_REPLY, PING, PONG, INFO, OK, ERR }
