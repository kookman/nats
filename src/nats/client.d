module nats.client;

import vibe.core.core;
import vibe.core.log;
import core.time;
import std.exception;

public import nats.interface_;
import nats.parser;

enum VERSION = "nats_v0.4.0";

// Allow quietening the debug logging from nats client
version (NatsClientQuiet) {}
else version = NatsClientLogging;

final class Nats
{
    import std.format: sformat;
    import std.socket: Address, parseAddress;
    import std.traits: Unqual;
    import eventcore.core: IOMode;
    import vibe.core.net: TCPConnection, connectTCP, WaitForDataStatus;
    import vibe.core.sync: TaskCondition, RecursiveTaskMutex, TaskMutex;
    import vibe.data.json: Json;
    import nbuff: Nbuff;

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
        NatsClientConfig config;
        config.natsUri = natsUri;
        config.clientId = name;
        this(config);
    }

    this(NatsClientConfig config) @trusted
    {
        import std.algorithm.searching: findSplit, startsWith;
        import std.format: format;
        import vibe.inet.url: URL;

        immutable ushort defaultPort = 4222;
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
        _flushMutex = new TaskMutex;
        _writeMutex = new RecursiveTaskMutex;
        _flushSync = new TaskCondition(_flushMutex);
        // reserve a sensible minimum amount of space for new subscriptions
        _subs.reserve(16);
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

    ~this() @trusted
    {
        version (NatsClientLogging) logDebug("nats client object destroyed!");
    }

    void connect() @safe
    {
        _connector = runTask(&connector);
    }


    bool connected() @safe const nothrow
    {
        return (_connState == NatsState.CONNECTED && _conn.connected);
    }


    const(Subscription[]) subscriptions() @safe const nothrow
    {
        return _subs;
    }


    const(Statistics) statistics() @safe const nothrow
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
            cmd = buffer.sformat!"UNSUB %s %s"(s.sid, msgsToDrain);			
        }
        else
        {
            s.closed = true;
            cmd = buffer.sformat!"UNSUB %s"(s.sid);			
        }
        write(cmd);
    }


    void publish(T)(scope const(char)[] subject, scope const(T)[] payload) @safe nothrow
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
   {
        sendPublish(subject, payload);
    }


    void publishRequest(T)(scope const(char)[] subject, scope const(T)[] payload, NatsHandler handler) @safe nothrow
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
    {
        import std.conv: to;
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
            write(PING);
        catch (Exception e) {
            logError("nats.client: Flush write error (%s). Disconnecting from Nats.", e.msg);
            disconnect();
        }
        _pingSent++;
        _flushSync.wait();
        _lastFlushRTT = MonoTime.currTime() - start;
        return _lastFlushRTT;
    }

    void disconnect() @safe nothrow
    {
        _connectMutex.lock();
        scope(exit) _connectMutex.unlock();
        
        if (_connState != NatsState.CLOSED) {
            logInfo("nats.client: Closing TCP connection to Nats server.");
            try 
                _conn.close();
            catch (Exception e) {
                logError("nats client: Error (%s) closing TCP connection!");
            }
            _connState = NatsState.CLOSED;
        }
    }

    private:

    enum OUTCMD_BUFSIZE = 256;

    TCPConnection 		 _conn;
    RecursiveTaskMutex	 _writeMutex;
    TaskMutex            _connectMutex;
    TaskMutex 			 _flushMutex;
    TaskCondition   	 _flushSync;
    Duration 			 _lastFlushRTT;
    Duration			 _heartbeatInterval;
    Duration             _reconnectInterval;
    Duration             _connectTimeout;
    Task                 _connector;
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
    ConnectInfo          _connectInfo;
    Json 				 _info;
    string               _host;
    ushort               _port;
    bool 				 _useTls;
    NatsState			 _connState;
    string               _inboxPrefix;
    NatsHandler[uint]    _inboxes;


    void connector() @safe
    {
        import vibe.data.json: serializeToJsonString;

        char[OUTCMD_BUFSIZE] buffer = void;
        char[] cmd;

        while (!connected() && _connState != NatsState.CLOSED)
        {
            _connectMutex.lock();
            scope(exit) _connectMutex.unlock();

            auto initialState = _connState;
            version (NatsClientLogging) logDebug("Connector: Establishing connection to NATS server (%s) port %s ...", _host, _port);
            try
                _conn = connectTCP(_host, _port, null, 0, 5.seconds);
            catch (Exception e) {
                version (NatsClientLogging) logDebug("nats.client Connector: Exception whilst attempting connect to Nats server, msg: %s", e.msg);
            }
            if (_conn.connected)
            {
                _conn.keepAlive(true);
                _conn.tcpNoDelay(true);
                _conn.readTimeout(10.seconds);
                _connState = NatsState.CONNECTED;
                // start the listener & heartbeater tasks		
                _listener = runTask(&listener);
                _heartbeater = runTask(&heartbeater);
                // send the CONNECT string
                cmd = buffer.sformat("CONNECT %s", serializeToJsonString(_connectInfo));
                version (NatsClientLogging) logDebug("Connector: Connected! Sending: %s", cmd);
                write(cmd);
                auto rtt = flush();
                version (NatsClientLogging) logDebug("Connector: Flush roundtrip (%s) completed. Nats connection ready.", rtt);
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
                version (NatsClientLogging) logDebug("nats.client Connector: Sending inbox subscription: %s", inbox.subject);
                sendSubscribe(inbox);
                if (initialState == NatsState.RECONNECTING && _subs.length > 1) 
                {
                    version (NatsClientLogging) logInfo("nats.client Connector: Re-sending active subscriptions after reconnection to Nats server.");
                    foreach (priorSubscription; _subs[1..$]) {
                        if (!priorSubscription.closed) sendSubscribe(priorSubscription);
                    }
                }
                // go async here until we need to reconnect...
                _listener.join();
                _heartbeater.join();
                _connState = NatsState.RECONNECTING;
                _pingSent = 0;
                _pongRecv = 0; 
            }
            else
                logWarn("nats.client Connector: Connection attempt to %s failed.", _host);
            sleep(_reconnectInterval);
        }
    }


    void listener() @safe
    {
        version (NatsClientLogging) logDebug("nats.client: listener task started.");

        enum size_t readSize = 512;
        Nbuff buffer;

        while(connected())
        {
            // task blocks here until we receive data. No timeout as that is handled
            // by the heartbeater task.
            auto result = _conn.waitForDataEx();
            if (result == WaitForDataStatus.dataAvailable)
            {
                auto readBuffer = Nbuff.get(readSize);
                auto readBufferSpace = () @trusted { return readBuffer.data(); }();
                immutable bytesRead = _conn.read(readBufferSpace, IOMode.once);
                _bytesReceived += bytesRead;
                // append this newly read chunk to the buffer and process
                buffer.append(readBuffer, bytesRead);
                size_t consumed = processNatsStream(buffer);
                version (NatsClientLogging) {
                    if (consumed < buffer.length)
                        logTrace("Fragment (length: %s) left in buffer. Consolidating with another read.", buffer.length - consumed);
                }
                // free fully processed messages from the buffer
                buffer.pop(consumed);
            }
            else if (result == WaitForDataStatus.noMoreData)
            {
                logError("nats.client: Listener read session closed unexpectedly: %s", result);
                if (connected()) {
                    // ensure underlying TCP connection is closed - state might not yet reflect this
                    _conn.close();  
                    _connState = NatsState.DISCONNECTED;
                }
            }
        }
        logWarn("nats.client: Nats session disconnected! Listener task terminating. Connector will attempt reconnect.");
    }


    void heartbeater() @safe
    {
        version (NatsClientLogging) logDebug("nats.client: heartbeater task started.");

        auto timer = createTimer(null);
        while(connected())
        {
            auto prevSent = _msgSent;
            auto prevRecv = _msgRecv;
            auto prevPing = _pingRecv;
            bool flushPending = false;
            timer.rearm(_heartbeatInterval, false);
            timer.wait();
            //sleep(_heartbeatInterval);
            if (!flushPending && _msgSent == prevSent && _msgRecv == prevRecv && _pingRecv == prevPing && connected()) {
                version (NatsClientLogging) logDebugV("nats.client: Nats connection idle for %s. Sending heartbeat.",
                    _heartbeatInterval);
                runTask({
                    flushPending = true;
                    auto rtt = flush();
                    version (NatsClientLogging) logDebugV("nats.client: Nats Heartbeat RTT: %s", rtt);
                    flushPending = false;
                });
            } else if (flushPending) {
                logError("nats.client: Heartbeat did not return within heartbeat interval (%s).", _heartbeatInterval);
                _conn.close();
                _connState = NatsState.DISCONNECTED;
                break;
            }
        }
        logWarn("nats.client: Nats session disconnected! Heartbeater task terminating.");
    }


    void sendPublish(T)(scope const(char)[] subject, scope const(T)[] payload, scope const(char)[] replySubject = null) @safe nothrow
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
    {
        char[OUTCMD_BUFSIZE] buffer = void;
        char[] cmd;
    
        try {
            cmd = buffer.sformat!"PUB %s %s %s"(subject, replySubject, payload.length);
            // ensure we don't interleave other writes between writing PUB command & payload
            _writeMutex.lock();
            scope(exit) _writeMutex.unlock();       
            write(cmd);
            write(payload);
            _msgSent++;
        }
        catch (Exception e) {
            logError("nats.client: Publish failed (%s), command (%s). Disconnecting from Nats.", e.msg, cmd);
            disconnect();
        }
    }


    void sendSubscribe(Subscription s) @safe nothrow
    {
        char[OUTCMD_BUFSIZE] buffer = void;
        char[] cmd;

        try {
            if (s.queueGroup)
                cmd = buffer.sformat!"SUB %s %s %s"(s.subject, s.queueGroup, s.sid);
            else
                cmd = buffer.sformat!"SUB %s %s"(s.subject, s.sid);
            write(cmd);
        }
        catch (Exception e) {
            logError("nats.client: Error (%s) subscribing on (%s). Disconnecting from Nats.", e.msg, s.subject);
            disconnect();
        }
    }


    void write(T)(scope const(T)[] buffer) @safe 
        if (is(Unqual!T == ubyte) || is(Unqual!T == char))
    {
        synchronized(_writeMutex)
        {
            version (NatsClientLogging) logTrace("nats.client write: %s", () @trusted { return cast(string)buffer; }());            
            auto bytesWritten = _conn.write(cast(const(ubyte)[]) buffer, IOMode.all);
            // nats protocol requires all writes to be separated by CRLF
            bytesWritten += _conn.write(CRLF, IOMode.all);
            if (bytesWritten != buffer.length + 2)
            {
                logError("nats.client: Error writing to Nats connection! Socket write error.");
            }
            _bytesSent += bytesWritten;
        }
    }


    size_t processNatsStream(ref Nbuff natsResponse) @safe
    {
        import std.algorithm.comparison: min;

        size_t consumed = 0;
        Subscription subscription;
    
        loop:
        while(consumed < natsResponse.length)
        {
            Msg msg;
            Nbuff msgPayload;

            if (consumed > 0)
                natsResponse.pop(consumed);
            auto responseData = () @trusted { return natsResponse.data().data(); }();
            version (NatsClientLogging)
                logTrace("Sending response slice length %d to parseNats.", responseData.length);
            consumed = parseNats(responseData, msg);

            final switch (msg.type)
            {	
                case NatsResponse.FRAGMENT:
                    if (consumed > 0) {
                        break;
                    } else {
                        break loop;
                    }
                        
                case NatsResponse.MSG:
                case NatsResponse.MSG_REPLY:
                    immutable alreadyRead = min(msg.length, natsResponse.length - consumed);
                    if (msg.length > alreadyRead)
                    {
                        version (NatsClientLogging) 
                            logTrace("MSG payload exceeds initial buffer (length: %s).", msg.length);
                        auto msgPayloadBuffer = Nbuff.get(msg.length - alreadyRead);
                        auto remainingPayload = () @trusted { return msgPayloadBuffer.data(); }();
                        immutable bytesRead = _conn.read(remainingPayload[0 .. msg.length - alreadyRead], IOMode.all);
                        if (bytesRead < msg.length - alreadyRead)
                        {
                            logError("nats.client: Message payload incomplete! (expected: %s bytes)", msg.length);
                        }
                        natsResponse.append(msgPayloadBuffer, bytesRead);
                        _bytesReceived += bytesRead;
                    }
                    msgPayload = natsResponse[consumed .. consumed + msg.length];
                    consumed += msg.length;
                    msg.payload = () @trusted { return msgPayload.data().data(); }();
                    _msgRecv++;
                    subscription = _subs[msg.sid];
                    subscription.msgsReceived++;
                    subscription.bytesReceived += msg.payload.length;
                    if (subscription.msgsReceived > subscription.msgsToExpire)
                        subscription.closed = true;
                    if (subscription.closed)
                        logWarn("nats.client: Discarding message received on closed subscription! (msg.subject: %s, subscription: %s)", 
                            msg.subject, subscription.subject);
                    else
                        // now call the message handler
                        // note: this is a synchronous callback - don't block the event loop for too long
                        // if necessary, copy what is needed out of the msg and send to a new Task or Thread
                        subscription.handler(msg);
                    continue loop;
                
                case NatsResponse.PING:
                    write(PONG);
                    _pongSent++;
                    version (NatsClientLogging) logDebugV("Pong sent.");
                    continue loop;
                
                case NatsResponse.PONG:
                    _pongRecv++;
                    if (_pongRecv == _pingSent) 
                        _flushSync.notify();
                    continue loop;

                case NatsResponse.OK:
                    version (NatsClientLogging) logDebug("Ok received.");
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
        return consumed;		
    }

    void processServerInfo(scope Msg msg) @safe
    {
        import vibe.data.json: parseJsonString;

        _info = parseJsonString(msg.payloadAsString);
    }

    void inboxHandler(scope Msg msg) @safe nothrow
    {
        import std.algorithm.searching: findSplitAfter;
        import std.conv: to;
        
        version (NatsClientLogging) 
            logDebugV("Inbox %s handler called with msg: %s", msg.subject, msg.payloadAsString);
        uint inbox;
        bool badResponse = false;
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
