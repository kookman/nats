module nats.client;

import vibe.core.core;
import vibe.core.log;
import core.time;
import std.exception;

public import nats.interface_;
import nats.parser;

enum VERSION = "nats_v0.5.2";

// Allow quietening the debug logging from nats client
version (NatsClientQuiet) {}
else version = NatsClientLogging;

final class Nats
{
    import std.algorithm.comparison: min;
    import std.algorithm.searching: findSplit, findSplitAfter, startsWith;
    import std.array: appender, Appender;
    import std.conv: to;
    import std.format: format, formattedWrite;
    import std.random: uniform;
    import std.socket: Address, parseAddress;
    import std.string: assumeUTF, representation;
    import std.traits: Unqual;
    import eventcore.core: IOMode;
    import vibe.core.net: TCPConnection, connectTCP, WaitForDataStatus;
    import vibe.core.stream: Stream;
    import vibe.core.sync: TaskCondition, RecursiveTaskMutex, TaskMutex;
    import vibe.data.json: Json, parseJsonString, serializeToJsonString;
    import vibe.inet.message: parseRFC5322Header;
    import vibe.inet.url: URL;
    import vibe.internal.interfaceproxy: InterfaceProxy;
    import vibe.stream.operations: readLine;
    import vibe.stream.tls: TLSContextKind, createTLSContext, createTLSStream;

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
        ulong payloadBytesReceived;
        ulong payloadBytesSent;
        ulong msgsReceived;
        ulong msgsSent;
    }

    this(string natsUri = "nats://127.0.0.1:4222", string name = null) @safe
    {
        NatsClientConfig config;
        config.natsUri = natsUri;
        config.clientId = name;
        this(config);
    }

    this(NatsClientConfig config) @safe
    {
        enum ushort defaultPort = 4222;
        uint schemaSkip;
    
        if (config.natsUri.startsWith("nats:"))
        {
            _useTls = false;
            schemaSkip = 4;
        }
        else if (config.natsUri.startsWith("tls:"))
        {
            _useTls = true;
            schemaSkip = 3;
        }

        string uri = schemaSkip ? format!"tcp%s"(config.natsUri[schemaSkip..$]) : config.natsUri;
        auto immutable url = URL(uri);

        _connectInfo.name = config.clientId;
        _connectInfo.user = url.username;
        _connectInfo.pass = url.password;
        _host = url.host;
        _port = (url.port == 0) ? defaultPort : url.port;
        _heartbeatInterval = config.heartbeatInterval;
        _reconnectInterval = config.reconnectInterval;
        _connectTimeout = config.connectTimeout;

        _connectMutex = new TaskMutex;
        _connStateChange = new TaskCondition(_connectMutex);
        _flushMutex = new TaskMutex;
        _writeMutex = new RecursiveTaskMutex;
        _flushSync = new TaskCondition(_flushMutex);
        // reserve a sensible minimum amount of space for new subscriptions
        _subs.reserve(16);
        // setup initial outbound NATS buffer size
        _outBuffer.reserve(64 * 1024);
    }

    unittest
    {
        NatsClientConfig testConfig;
        testConfig.natsUri = "nats://kookman:pass@127.0.0.1:4222";
        auto nats = new Nats(testConfig);
        assert (nats._host == "127.0.0.1");
        assert (nats._port == 4222);
        assert (nats._connectInfo.user == "kookman");
        assert (nats._connectInfo.pass == "pass");
    }

    ~this() @safe
    {
        version (NatsClientLogging) logDebug("nats client object destroyed!");
    }

    void connect() @safe
    {
        _connector = runTask(&connector);
    }


    bool connected() @safe const nothrow
    {
        return (_connState == NatsState.CONNECTED);
    }


    const(Subscription[]) subscriptions() @safe const nothrow
    {
        return _subs;
    }


    const(Statistics) statistics() @safe const nothrow
    {
        Statistics stats;

        stats.payloadBytesReceived = _payloadBytesReceived;
        stats.payloadBytesSent = _payloadBytesSent;
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
        if (msgsToDrain > 0)
        {
            s.msgsToExpire = s.msgsReceived + msgsToDrain;
            _outBuffer.formattedWrite!"UNSUB %s %s\r\n"(s.sid, msgsToDrain);			
        }
        else
        {
            s.closed = true;
            _outBuffer.formattedWrite!"UNSUB %s\r\n"(s.sid);			
        }
        send();
    }


    void publish(T)(scope const(char)[] subject, scope const(T)[] payload) @safe nothrow
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
   {
        sendPublish(subject, payload);
    }


    void publishRequest(T)(scope const(char)[] subject, scope const(T)[] payload, NatsHandler handler) @safe nothrow
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
    {
        auto inboxId = to!string(_msgSent);
        _inboxes[_msgSent] = handler;
        auto replyInbox = _inboxPrefix ~ inboxId;
        sendPublish(subject, payload, replyInbox);
    }


    Duration flush(Duration timeout = 5.seconds) @safe nothrow
    {
        _flushMutex.lock();
        scope(exit) _flushMutex.unlock();
        
        auto start = MonoTime.currTime();
        try 
            write("PING\r\n");
        catch (Exception e) {
            logError("nats.client: Flush write error (%s)", e.msg);
            disconnect();
        }
        _pingSent++;
        _flushSync.wait();
        _lastFlushRTT = MonoTime.currTime() - start;
        return _lastFlushRTT;
    }

    void disconnect() @safe nothrow
    {
        if (_connState != NatsState.DISCONNECTED) {
            logWarn("nats.client: Disconnecting from Nats.");
            _connState = NatsState.DISCONNECTED;
            _connStateChange.notify();
        }
    }

    void close() @safe nothrow
    {
        if (_connState != NatsState.CLOSED) {
            logInfo("nats.client: Closing TCP connection to Nats server.");
            try 
                _conn.close();
            catch (Exception e) {
                logError("nats.client: Error (%s) closing TCP connection!");
            }
            _connState = NatsState.CLOSED;
            _connStateChange.notify();
        }
    }

    private:

    enum OUTCMD_BUFSIZE = 256;

    TCPConnection 		 _conn;
    InterfaceProxy!Stream _natsStream;
    RecursiveTaskMutex	 _writeMutex;
    TaskMutex            _connectMutex;
    TaskCondition        _connStateChange;
    TaskMutex 			 _flushMutex;
    TaskCondition   	 _flushSync;
    Duration 			 _lastFlushRTT;
    Duration			 _heartbeatInterval;
    Duration             _reconnectInterval;
    Duration             _connectTimeout;
    Task                 _connector;
    Task                 _heartbeater;
    Task                 _listener;
    ulong                _payloadBytesSent;
    ulong                _payloadBytesReceived;
    uint                 _msgSent;
    uint                 _msgRecv;
    uint                 _pingSent;
    uint                 _pongRecv;
    uint                 _pingRecv;
    uint                 _pongSent;
    Subscription[]       _subs;
    ConnectInfo          _connectInfo;
    Json 				 _info;
    string               _host;
    ushort               _port;
    bool 				 _useTls;
    NatsState			 _connState;
    string               _inboxPrefix;
    NatsHandler[uint]    _inboxes;
    ubyte[]              _payloadBuffer;
    Appender!(ubyte[])   _outBuffer;


    void setupNatsConnection() @safe nothrow
    {
        _conn.keepAlive(true);
        _conn.tcpNoDelay(true);
        // reset _pingSent & _pong Recv to enable flush syncing
        _pingSent, _pongRecv = 0;
        // send the CONNECT string
        try {
            _conn.readTimeout(10.seconds);
            _outBuffer.formattedWrite("CONNECT %s\r\n", serializeToJsonString(_connectInfo));
            version (NatsClientLogging)
                logDebug("nats.client: Socket connected. Sending: %s", _outBuffer[].assumeUTF);
            send();
        }
        catch (Exception e) {
            logError("nats.client: Failed to send initial CONNECT string (%s).", e.msg);
            return;
        }
        auto rtt = flush();
        version (NatsClientLogging)
            logDebug("nats.client: Flush roundtrip (%s) completed.", rtt);
        // create a connection specific inbox subscription
        _inboxPrefix = "_INBOX_" ~ _conn.localAddress.toString() ~ "_.";
        auto inbox = new Subscription;
        inbox.subject = _inboxPrefix ~ "*";
        inbox.sid = 0;
        inbox.handler = &inboxHandler;
        //_subs[0] is always my _INBOX_ so just replace any existing one
        if (_subs.length == 0) 
            _subs ~= inbox;
        else
            _subs[0] = inbox;
        version (NatsClientLogging)
            logDebug("nats.client: Setting up inbox subscription: %s", inbox.subject);
        sendSubscribe(inbox);
        if (_subs.length > 1) 
        {
            version (NatsClientLogging)
                logDebug("nats.client: Re-sending active subscriptions after reconnection to Nats server.");
            foreach (priorSubscription; _subs[1..$]) {
                if (!priorSubscription.closed) sendSubscribe(priorSubscription);
            }
        }
        _connState = NatsState.CONNECTED;
    }

    void connector() @safe nothrow
    {
        auto reconnectTimer = createTimer(null);
        _connectMutex.lock();
        scope(exit) _connectMutex.unlock();
        
        connector:
        while (_connState != NatsState.CLOSED) {
            final switch (_connState) {
                case NatsState.INIT:
                case NatsState.RECONNECTING:
                    version (NatsClientLogging) logDiagnostic(
                        "nats.client: Establishing connection to NATS server (%s) port %s ...", _host, _port);
                    try {
                        if (_conn.connected)
                            _conn.close();
                        _connState = NatsState.CONNECTING;
                        _conn = connectTCP(_host, _port, null, 0, 5.seconds);
                        if (_useTls) {
                            // wrap the TCP connection in a TLS stream
                            auto tlsContext = createTLSContext(TLSContextKind.client);
                            _natsStream = createTLSStream(_conn, tlsContext);
                        } else {
                            _natsStream = _conn;
                        }
                    }
                    catch (Exception e) {
                        logWarn("nats.client: Exception whilst attempting connect to Nats server, msg: %s", e.msg);
                    }
                    break;
                
                case NatsState.CONNECTING:
                    if (_conn.connected) {
                        _listener = runTask(&listener);
                        setupNatsConnection();
                    } else {
                        logWarn("nats.client Connector: Connection attempt to %s failed.", _host);
                        _connState = NatsState.DISCONNECTED;
                    }
                    break;                   

                case NatsState.DISCONNECTED:
                    // wait for an interval + random extra delay to avoid thundering herd on Nats server down
                    try {
                        Duration delay = _reconnectInterval + uniform(0, 1000).msecs;
                        logInfo("nats.client: Waiting %s before attempting reconnect.", delay);
                        reconnectTimer.rearm(delay, false);
                        reconnectTimer.wait();
                    }
                    catch (Exception e) {
                        logWarn("nats.client: Reconnect timer interrupted.");
                        break;
                    }
                    _connState = NatsState.RECONNECTING;
                    break;

                case NatsState.CONNECTED:
                    logInfo("nats.client: Nats connection ready.");
                    _heartbeater = runTask(&heartbeater);
                    _connStateChange.wait();
                    break;

                case NatsState.CLOSED:
                    break connector;
            }
        }
        version (NatsClientLogging) logDiagnostic("nats.client: Connector task terminating.");
    }


    void listener() @safe nothrow
    {
        version (NatsClientLogging) logDiagnostic("nats.client: listener task started.");
        try {
            processNatsStream();
        }
        catch (Exception e) {
            logWarn("nats.client: Nats session disconnected! (%s).", e.msg);
        }
        disconnect();
        version (NatsClientLogging)
            logDiagnostic("nats.client: Listener task terminating. Connector will attempt reconnect.");
    }


    void heartbeater() @safe nothrow
    {
        version (NatsClientLogging) logDiagnostic("nats.client: heartbeater task started.");

        auto timer = createTimer(null);
        bool flushPending = false;

        void heartbeat() @safe nothrow
        {
            flushPending = true;
            scope(exit) flushPending = false;
            auto t = runTask(() @safe nothrow {
                auto rtt = flush();
                version (NatsClientLogging)
                    logDebug("nats.client: Nats Heartbeat RTT: %s", rtt);
            });
            try
                t.join();
            catch (Exception e) {
                logWarn("nats.client: Heartbeat flush failed to sync.");
                disconnect();
            }
        }

        while(_connState == NatsState.CONNECTED) {
            auto prevSent = _msgSent + _pingSent;
            auto prevRecv = _msgRecv + _pingRecv;
            timer.rearm(_heartbeatInterval, false);
            try
                timer.wait();
            catch (Exception e) {
                logWarn("nats.client: Heartbeat timer interrupted.");
                break;
            }
            if (_msgSent + _pingSent == prevSent && _msgRecv + _pingRecv == prevRecv
                    && _connState == NatsState.CONNECTED) {
                version (NatsClientLogging) 
                    logDebugV("nats.client: Nats connection idle for %s. Sending heartbeat.", _heartbeatInterval);
                runTask(&heartbeat);
            } else if (flushPending) {
                logError("nats.client: Heartbeat did not return within heartbeat interval (%s).", _heartbeatInterval);
                disconnect();
            }
        }
        version (NatsClientLogging)
            logDiagnostic("nats.client: Nats session not connected! Heartbeater task terminating.");
    }


    void sendPublish(T)(scope const(char)[] subject, scope const(T)[] payload, 
                            scope const(char)[] replySubject = null) @safe nothrow
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
    // fixme: to allow for 
    {
        try
            if (replySubject.length)
                _outBuffer.formattedWrite!"PUB %s %s %s\r\n"(subject, replySubject, payload.length);
            else
                _outBuffer.formattedWrite!"PUB %s %s\r\n"(subject, payload.length);
        catch (Exception e)
            logError("nats.client: Exception writing PUB cmd to output buffer. (%s)", e.msg);
        if (payload.length + 2 < _outBuffer.capacity) {
            static if (is(Unqual!T == char))
                _outBuffer ~= payload.representation;
            else
                _outBuffer ~= payload;
            _outBuffer ~= CRLF;
            send();
        } else {
            try {
                // ensure we don't interleave other writes between writing PUB command & payload
                _writeMutex.lock();
                scope(exit) _writeMutex.unlock();
                send();
                write(payload);
                write(CRLF);
            }       
            catch (Exception e) {
                logError("nats.client: Publish failed (%s)", e.msg);
                disconnect();
            }
        }
        _payloadBytesSent += payload.length;
        _msgSent++;
    }


    void sendSubscribe(Subscription s) @safe nothrow
    {
        try {
            if (s.queueGroup)
                _outBuffer.formattedWrite!"SUB %s %s %s\r\n"(s.subject, s.queueGroup, s.sid);
            else
                _outBuffer.formattedWrite!"SUB %s %s\r\n"(s.subject, s.sid);
        }
        catch (Exception e) {
            logError("nats.client: Error writing SUB command to output buffer. (%s)", e.msg);
        }
        send();
    }


    /// Send the accumulated message(s) in the outbound buffer immediately to the stream
    void send() @safe nothrow
    {
        ulong bytesWritten, bufferLen;
        try {
            synchronized(_writeMutex) {
                bufferLen = _outBuffer[].length;
                version (NatsClientLogging)
                    logTrace("nats.client outBuffer send, length: %d, snippet: %s ...", 
                            bufferLen, _outBuffer[].assumeUTF[0 .. min(100, bufferLen)]);
                bytesWritten = _natsStream.write(_outBuffer[], IOMode.all);
            }
        }
        catch (Exception e)
            logError("nats.client: Stream write failed (%s)", e.msg);
        _outBuffer.clear();
        if (bytesWritten != bufferLen)
        {
            logError("nats.client: Error writing to Nats stream. Expected to write %d bytes, wrote %d.");
        }
    }


    /// Write a buffer of bytes immediately to the stream
    void write(T)(scope const(T)[] buffer) @safe
        if (is(Unqual!T == char) || is(Unqual!T == ubyte))
    {
        synchronized(_writeMutex)
        {
            version (NatsClientLogging) logTrace("nats.client stream write, length: %d", buffer.length);
            static if (is(Unqual!T == char))
                auto bytesWritten = _natsStream.write(buffer.representation, IOMode.all);
            else
                auto bytesWritten = _natsStream.write(buffer, IOMode.all);
            if (bytesWritten != buffer.length)
            {
                logError("nats.client: Error writing to Nats connection! Socket write error.");
            }
        }
    }


    void processNatsStream() @safe
    {
        enum maxNatsProtocolLine = 2048;
        enum maxStatusLength = 512;
        auto protocolLine = appender!(ubyte[]);
        protocolLine.reserve(maxNatsProtocolLine);
        auto statusLine = appender!(ubyte[]);
        statusLine.reserve(maxStatusLength);
        Subscription subscription;
    
        loop:
        // main message processing loop
        while (_connState == NatsState.CONNECTING || _connState == NatsState.CONNECTED) {
            // idle loop will block here until data available or socket drops
            auto result = _conn.waitForDataEx();
            if (result == WaitForDataStatus.noMoreData) {
                throw new NatsException("Nats TCP connection lost.");
            }

            Msg msg;
            protocolLine.clear();
            statusLine.clear();
            
            _natsStream.readLine(protocolLine);
            // skip any blank lines (ie lines with only a CRLF)
            if (protocolLine[].length == 0) 
                continue loop;
            version (NatsClientLogging)
                logTrace("Sending NATS protocol line length %d to parseNats.", protocolLine[].length);
            parseNats(msg, protocolLine[]); 

            final switch (msg.type)
            {	
                // note: Using streams means receiving a FRAGMENT should only occur in an error condition
                case NatsResponse.FRAGMENT:
                    break loop;
                        
                case NatsResponse.MSG:
                case NatsResponse.MSG_REPLY:
                    // read headers if it is a HMSG
                    if (msg.headersLength) {
                        _natsStream.readLine(statusLine);
                        msg.headerStatusLine = statusLine[].assumeUTF;
                        if (msg.headersLength - msg.headerStatusLine.length > 4) {
                            // 4 is the length of the trailing double CRLF so there are more headers
                            _natsStream.parseRFC5322Header(msg.headers);
                        }
                    }
                    immutable expectedPayload = msg.length - msg.headersLength;
                    immutable bytesRead = _natsStream.read(_payloadBuffer[0 .. expectedPayload], IOMode.all);
                    if (bytesRead < expectedPayload) {
                        logError("nats.client: Message payload incomplete! (expected: %s bytes)", expectedPayload);
                    }
                    msg.payload = _payloadBuffer[0 .. expectedPayload];
                    _msgRecv++;
                    _payloadBytesReceived += expectedPayload;
                    subscription = _subs[msg.sid];
                    subscription.msgsReceived++;
                    subscription.bytesReceived += expectedPayload;
                    if (subscription.msgsReceived > subscription.msgsToExpire)
                        subscription.closed = true;
                    if (subscription.closed)
                        logWarn("nats.client: Discarding message received on closed subscription!"
                            ~ " (msg.subject: %s, subscription: %s)", msg.subject, subscription.subject);
                    else
                        // now call the message handler
                        // note: this is a synchronous callback - don't block the event loop for too long
                        // if necessary, copy what is needed out of the msg and send to a new Task or Thread
                        subscription.handler(msg);
                    continue loop;
                
                case NatsResponse.PING:
                    write("PONG\r\n");
                    _pongSent++;
                    version (NatsClientLogging) logDebugV("Pong sent.");
                    continue loop;
                
                case NatsResponse.PONG:
                    _pongRecv++;
                    if (_pongRecv == _pingSent) 
                        _flushSync.notify();
                    continue loop;

                case NatsResponse.OK:
                    version (NatsClientLogging) logDebug("Nats Ok received.");
                    continue loop;

                case NatsResponse.INFO:
                    version (NatsClientLogging) logDebug("Server INFO msg: %s", msg.payloadAsString);
                    processServerInfo(msg);
                    continue loop;

                case NatsResponse.ERR:
                    logError("nats.client: Server ERR msg: %s", msg.payloadAsString);
                    continue loop;
            }
        }
    }

    void processServerInfo(scope Msg msg) @safe
    {
        // copy the message payload as we will be retaining it
        string serverjson = msg.payloadAsString.idup;
        _info = parseJsonString(serverjson);
        // allocate the payload buffer according to largest allowed
        ulong maxPayloadSize = _info["max_payload"].get!ulong;
        if (maxPayloadSize > _payloadBuffer.length) {
            _payloadBuffer = new ubyte[maxPayloadSize];
        }
    }

    void inboxHandler(scope Msg msg) @safe nothrow
    {
        uint inbox;
        bool badResponse = false;
        version (NatsClientLogging) 
            logDebugV("Inbox %s handler called with msg: %s", msg.subject, msg.payloadAsString);
        try {
            auto findInbox = msg.subject.findSplitAfter("_.");
            if (!findInbox) 
                badResponse = true;
            else
                inbox = findInbox[1].to!uint;
        }
        catch (Exception e) {
            badResponse = true;
        }
        if (badResponse) {
            logWarn("nats.client: Unexpected msg (%s) received in inbox. Discarding.", msg.subject);
            return;
        }
        auto p_handler = (inbox in _inboxes);
        if (p_handler !is null) {
            (*p_handler)(msg);
            _inboxes.remove(inbox);
        }
        else {
            logWarn("nats.client: No response handler for response in inbox: %s. Response discarded.", 
                msg.subject);
        }
    }
}
