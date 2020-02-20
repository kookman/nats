module nats.client;

import vibe.core.core;
import vibe.core.log;
import core.time;
import std.exception;

public import nats.interface_;
import nats.parser;

/* TODO:
 |*| Implement SUB api
 |*| Implement PUB api
 |*| Implement flush logic
 | | Implement request-response subscriptions
 |*| Support proper connect options
 | | Support reconnect logic
 |*| Support large messages
 |*| Support distributed queues (subscriber groups)
 | | ? Support (de)serialisation protocols: msgpack, cerealed, none (passthru ubyte[])
*/

enum VERSION = import("VERSION");


final class Nats
{
	import std.experimental.allocator.mallocator: Mallocator;
	import std.format: sformat;
	import std.socket: Address, parseAddress;
	import std.traits: Unqual;
	import eventcore.core: IOMode;
	import vibe.core.net: TCPConnection, connectTCP, WaitForDataStatus;
	import vibe.core.sync: LocalManualEvent, RecursiveTaskMutex, TaskMutex,
			createManualEvent;
	import vibe.data.json: Json;

	public:

	static natsClientVersion = VERSION;

	static struct ConnectInfo
	{
		string name;
		string lang = "dlang";
		string version_ = VERSION;
		string user;
		string pass;
		bool verbose;
		bool pedantic;
	}

	static struct Statistics
	{
		ulong bytesReceived;
		ulong bytesSent;
		ulong msgsReceived;
		ulong msgsSent;
	}

	this(string natsUri = "nats://127.0.0.1:4222", string name = null) @trusted
	{
		import std.algorithm.searching: findSplit, startsWith;
		import std.format: format;
		import vibe.inet.url: URL;

		ushort defaultPort = 4222;
		uint schemaSkip;
	
		if (natsUri.startsWith("nats:"))
		{
			_useTls = false;
			schemaSkip = 4;
		}
		else if (natsUri.startsWith("tls:"))
		{
			_useTls = true;
			schemaSkip = 3;
		}

		string uri = schemaSkip ? format!"tcp%s"(natsUri[schemaSkip..$]) : natsUri;
		auto url = URL(uri);

		_connectInfo.name = name;
		_connectInfo.user = url.username;
		_connectInfo.pass = url.password;
		_host = url.host;
		_port = (url.port == 0) ? defaultPort : url.port;
		_flushMutex = new TaskMutex;
		_writeMutex = new RecursiveTaskMutex;
		_readIdle = createManualEvent();
		_flushSync = createManualEvent();
		// reserve a sensible minimum amount of space for new subscriptions
		_subs.reserve(16);
		_protocolBuffer = cast(ubyte[]) Mallocator.instance.allocate(PROTOCOL_BUFSIZE);
	}

	unittest
	{
		auto nats = new Nats("nats://kookman:pass@127.0.0.1:4222");
		assert (nats._host == "127.0.0.1");
		assert (nats._port == 4222);
		assert (nats._connectInfo.user == "kookman");
		assert (nats._connectInfo.pass == "pass");
	}

	~this() @trusted
	{
		Mallocator.instance.deallocate(_protocolBuffer);
		if (_largePayloadBuffer)
			Mallocator.instance.deallocate(_largePayloadBuffer);
		logInfo("nats client object destroyed!");
	}

	void connect() @safe
	{
		runTask(&connector, 15.seconds);
	}


	bool connected() @safe nothrow
	{
		return (_connState == NatsState.CONNECTED && _conn.connected);
	}


	const(Subscription[]) subscriptions() @safe const
	{
		return _subs;
	}


	const(Statistics) statistics() @safe const
	{
		Statistics stats;

		stats.bytesReceived = _bytesReceived;
		stats.bytesSent = _bytesSent;
		stats.msgsReceived = _msgRecv;
		stats.msgsSent = _msgSent;
		return stats;
	}  


	Subscription subscribe(string subject, NatsHandler handler, 
		string queueGroup = null) @safe
	{
		Subscription s;

		s = new Subscription;
		s.subject = subject;
		s.sid = cast(uint)_subs.length;
		s.handler = handler;
		s.queueGroup = queueGroup;
		_subs ~= s;
		sendSubscribe(s);
		return s;
	}


	void unsubscribe(Subscription s, uint msgsToDrain = 0) @safe
	{
		char[OUTCMD_BUFSIZE] buffer = void;
		char[] cmd;

		if (msgsToDrain > 0)
		{
			s.msgsToExpire = s.msgsReceived + msgsToDrain;
			cmd = buffer.sformat!"UNSUB %s %s\r\n"(s.sid, msgsToDrain);			
		}
		else
		{
			s.closed = true;
			cmd = buffer.sformat!"UNSUB %s\r\n"(s.sid);			
		}
		write(cmd);
	}


	void publish(scope string subject, scope const ubyte[] payload) @safe
	{
		char[OUTCMD_BUFSIZE] buffer = void;
		char[] cmd;
	
		cmd = buffer.sformat!"PUB %s %s\r\n"(subject, payload.length);
		// ensure we don't yield between writing PUB command & payload
		synchronized(_writeMutex)
		{
			write(cmd);
			write(payload);
			write(CRLF);
			_msgSent++;
		}
	}


	Duration flush(Duration timeout = 5.seconds) @safe
	{
		synchronized(_flushMutex)
		{
			_lastFlush = MonoTime.currTime();
			auto syncEvents = _flushSync.emitCount();
			write(PING);
			_pingSent++;
			_flushSync.wait(timeout, syncEvents);
			if (_flushSync.emitCount() == syncEvents)
			{
				logError("Flush timeout. Disconnected?");
				if (!connected())
					_connState = NatsState.DISCONNECTED;
			}
		}
		return MonoTime.currTime() - _lastFlush;
	}


	private:

	enum PROTOCOL_BUFSIZE = 4096;
	enum OUTCMD_BUFSIZE = 256;

	TCPConnection 		 _conn;
	RecursiveTaskMutex	 _writeMutex;
	TaskMutex 			 _flushMutex;
	LocalManualEvent	 _readIdle;
	LocalManualEvent	 _flushSync;
	MonoTime 			 _lastFlush;
	Task                 _heartbeater;
	Task                 _listener;
	ulong                _bytesSent;
	ulong                _bytesReceived;
	uint                 _msgSent;
	uint                 _msgRecv;
	uint                 _pingSent;
	uint                 _pongRecv;
	uint                 _pingRecv;
	uint                 _pongSent;
	Subscription[]       _subs;
	ubyte[]		         _largePayloadBuffer;
	ubyte[]              _protocolBuffer;
	ulong 				 _fragmentSize;
	ConnectInfo          _connectInfo;
	Json 				 _info;
	string               _host;
	ushort               _port;
	bool 				 _useTls;
	NatsState			 _connState;


	void connector(Duration reconnectInterval = 15.seconds) @safe
	{
		import vibe.data.json: serializeToJsonString;

		char[OUTCMD_BUFSIZE] buffer = void;
		char[] cmd;

		while (_connState != NatsState.CLOSED)
		{
			auto initialState = _connState;
			logDebug("Connector: Establishing connection to NATS server (%s) port %s ...", _host, _port);
			_conn = connectTCP(_host, _port, null, 0, 5.seconds);
			if (_conn)
			{
				_conn.keepAlive(true);
				_conn.tcpNoDelay(true);
				_conn.readTimeout(10.seconds);
				_connState = NatsState.CONNECTED;
				// start the listener & heartbeater tasks		
				_listener = runTask(&listener, 5.seconds);
				_heartbeater = runTask(&heartbeater, 5.seconds);
				// send the CONNECT string
				cmd = buffer.sformat("CONNECT %s\r\n", serializeToJsonString(_connectInfo));
				logDebug("Connector: Connected! Sending: %s", cmd[0..$-2]);
				write(cmd);
				auto rtt = flush();
				logDebug("Connector: Flush roundtrip (%s) completed. Nats connection ready.", rtt);
				// create a connection specific inbox subscription
				auto inbox = new Subscription;
				inbox.subject = "_INBOX_" ~ _conn.localAddress.toString();
				inbox.sid = 0;
				inbox.handler = &inboxHandler;
				//_subs[0] is always my _INBOX_ so just replace any existing one
				if (_subs.length == 0) 
					_subs ~= inbox;
				else
					_subs[0] = inbox;
				logDebug("Connector: Sending inbox subscription: %s", inbox.subject);
				sendSubscribe(inbox);
				if (initialState == NatsState.RECONNECTING)
				{
					logInfo("Connector: This is the part where we re-send the subscriptions etc.");
					//TODO: Fix this
				}
				// go async here until we need to reconnect...
				_listener.join();
				_heartbeater.join();
				_connState = NatsState.RECONNECTING;
				_pingSent = 0;
				_pongRecv = 0; 
			}
			else
				logWarn("Connector: Connection attempt to %s failed.", _host);
			sleep(reconnectInterval);
		}
	}


	void listener(Duration heartbeatInterval = 5.seconds) @safe
	{
		logDebug("NATS listener task started.");
		while(connected())
		{
			auto result = _conn.waitForDataEx(heartbeatInterval);
			if (result == WaitForDataStatus.dataAvailable)
			{
				ubyte[] buffer = _protocolBuffer[_fragmentSize..$];
				auto bytesRead = _conn.read(buffer, IOMode.once);
				_bytesReceived += bytesRead;
				processNatsResponse(_protocolBuffer[0.._fragmentSize+bytesRead]);
			}
			else if (result == WaitForDataStatus.timeout)
			{
				logDebugV("Listener idle: Notifying heartbeater.");
				_readIdle.emit();
			}
			else if (result == WaitForDataStatus.noMoreData)
			{
				logError("Listener read session closed: %s", result);
				_connState = NatsState.DISCONNECTED;
			}
		}
		logWarn("Nats session disconnected! Listener task terminating. Connector will attempt reconnect.");
	}


	void heartbeater(Duration heartbeatInterval = 5.seconds) @safe
	{
		while(connected())
		{
			auto idleCount = _readIdle.emitCount();
			_readIdle.wait(heartbeatInterval, idleCount);
			if (MonoTime.currTime() - _lastFlush > heartbeatInterval)
			{
				auto rtt = flush();
				logDebugV("Heartbeat RTT: %s", rtt);
			}
		}
		logWarn("Nats session disconnected! Heartbeater task terminating.");
	}


	void sendSubscribe(Subscription s) @safe
	{
		char[OUTCMD_BUFSIZE] buffer = void;
		char[] cmd;

		if (s.queueGroup)
			cmd = buffer.sformat!"SUB %s %s %s\r\n"(s.subject, s.queueGroup, s.sid);
		else
			cmd = buffer.sformat!"SUB %s %s\r\n"(s.subject, s.sid);
		write(cmd);
	}


	void write(T)(inout T[] buffer) @safe
		if (is(Unqual!T == ubyte) || is(Unqual!T == char))
	{
		synchronized(_writeMutex)
		{
			auto bytesWritten = _conn.write(cast(const(ubyte)[]) buffer, IOMode.all);
			if (bytesWritten == buffer.length)
				logTrace("Write ok. %s bytes written.", bytesWritten);
			else 
				logError("Error writing to Nats connection, status: %s", bytesWritten);
			_bytesSent += bytesWritten;
		}
	}


	void processNatsResponse(const(ubyte)[] response) @safe
	{
		import std.algorithm.mutation: copy;

		size_t consumed;
		ubyte[] remaining, payload;
		Subscription subscription;
	
		_fragmentSize = 0;
		while(consumed < response.length)
		{
			auto msg = parseNats(response[consumed..$]);
			consumed += msg.consumed;
			logTrace("Consumed: %s", consumed);

			final switch (msg.type)
			{	
				case NatsResponse.FRAGMENT:
					_fragmentSize = response.length - consumed;
					logTrace("Fragment (length: %s) left in buffer. Consolidating with another read.", _fragmentSize);
					remaining = copy(response[consumed..$], _protocolBuffer);
					break;

				case NatsResponse.MSG:
				case NatsResponse.MSG_REPLY:
					if (msg.length > msg.payload.length)
					{
						auto payloadRead = response.length - consumed;
						logTrace("MSG payload exceeds initial buffer (length: %s).", msg.length);
						remaining = copy(response[consumed..$], _largePayloadBuffer);
						payload = remaining[0..msg.length + 2 - payloadRead]; 
						auto bytesRead = _conn.read(payload, IOMode.all);
						// should only reach this point once we have completed read successfully
						if (bytesRead < remaining.length)
						{
							logError("Message payload incomplete! (expected: %s)", msg.length + 2);
							throw new NatsProtocolException("Protocol error while expecting MSG payload.");
						}
						_bytesReceived += bytesRead;
						consumed = response.length;
						msg.payload = _largePayloadBuffer[0..msg.length];
					}
					_msgRecv++;
					subscription = _subs[msg.sid];
					subscription.msgsReceived++;
					subscription.bytesReceived += msg.payload.length;
					if (subscription.msgsReceived > subscription.msgsToExpire)
						subscription.closed = true;
					if (subscription.closed)
						logWarn("Message received on closed subscription! (msg.subject: %s, subscription: %s)", msg.subject, subscription.subject);
					else
						subscription.handler(msg);
					continue;
				
				case NatsResponse.PING:
					write(PONG);
					_pongSent++;
					logDebugV("Pong sent.");
					continue;
				
				case NatsResponse.PONG:
					_pongRecv++;
					if (_pongRecv == _pingSent) 
						_flushSync.emit();
					continue;

				case NatsResponse.OK:
					logDebug("Ok received.");
					continue;

				case NatsResponse.INFO:
					logDebug("Server INFO msg: %s", msg.payloadAsString);
					processServerInfo(msg);
					continue;

				case NatsResponse.ERR:
					logError("Server ERR msg: %s", msg.payloadAsString);
					continue;
			}
		}		
	}

	void processServerInfo(scope Msg msg) @trusted
	{
		import vibe.data.json: parseJsonString;

		_info = parseJsonString(msg.payloadAsString);
		auto maxPayload = _info["max_payload"].get!uint;
		if (maxPayload != _largePayloadBuffer.length)
		{
			if (_largePayloadBuffer)
				Mallocator.instance.deallocate(_largePayloadBuffer);
			logDebug("Allocating _largePayloadBuffer for max %s bytes.", maxPayload);
			_largePayloadBuffer = cast(ubyte[]) Mallocator.instance.allocate(maxPayload);
		}
	}

	void inboxHandler(scope Msg msg) @safe
	{
		logDebugV("Inbox %s handler called with msg: %s", msg.subject, msg.payloadAsString);
	}
}