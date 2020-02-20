module nats.interface_;

struct Msg
{
	string         subject;
	string         replySubject;
	uint           sid;
	uint           length;
	Subscription   subs;
	const(ubyte)[] payload;
	size_t         consumed;
	NatsResponse   type;

	string payloadAsString() @trusted
	{
		return cast(string)payload;
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


enum NatsState : byte { CONNECTING, CONNECTED, DISCONNECTED, RECONNECTING, CLOSED }

enum NatsResponse : byte { FRAGMENT, MSG, MSG_REPLY, PING, PONG, INFO, OK, ERR }
