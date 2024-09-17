module nats.parser;

import std.exception: assumeUnique;
import std.string: assumeUTF, representation;

import nats.interface_;

/* Nats protocol parsing implementation */

package:

enum MSG = "MSG".representation;
enum PING = "PING".representation;
enum PONG = "PONG".representation;
enum OK = "+OK".representation;
enum INFO = "INFO".representation;
enum ERR = "-ERR".representation;
enum CRLF = "\r\n".representation;
enum SPACE = " ".representation;
enum TAB = "\t".representation;


size_t parseNats(return ref Msg msg, const(ubyte)[] response) @safe
{
    import std.algorithm.comparison: equal;
    import std.algorithm.searching: findSplitAfter, startsWith;
    import std.algorithm.iteration: splitter;
    import std.ascii: isDigit;
    import std.conv: to;

    size_t consumed = 0;
    
    auto fragments = response.findSplitAfter(CRLF);
    if (!fragments)
    {
        // we don't have a full NATS protocol line, only a fragment
        msg.type = NatsResponse.FRAGMENT;
        return consumed;
    }
    auto protocolLine = fragments[0];
    if (protocolLine.length == 2)
    {
        // drop a line consisting only of leading CRLF
        msg.type = NatsResponse.FRAGMENT;
        return consumed + 2;
    }  
    auto remaining = fragments[1];
    consumed = response.length - remaining.length;
 
    if (protocolLine.startsWith(MSG))
    {
        auto tokens = protocolLine[4..$].assumeUTF.splitter;
        msg.subject = tokens.front;
        tokens.popFront();
        msg.sid = tokens.front.to!uint;
        tokens.popFront();
        if (!tokens.front[0].isDigit)
        {
            msg.type = NatsResponse.MSG_REPLY;
            msg.replySubject = tokens.front;
            tokens.popFront();
        }
        else
        {
            msg.type = NatsResponse.MSG;
        }
        msg.length = tokens.front.to!uint;
    }
    else if (protocolLine.startsWith(PONG))
    {
        msg.type = NatsResponse.PONG;
    }
    else if (protocolLine.startsWith(PING))
    {
        msg.type = NatsResponse.PING;
    }
    else if (protocolLine.startsWith(OK))
    {
        msg.type = NatsResponse.OK;
    }
    else if (protocolLine.startsWith(INFO))
    {
        msg.type = NatsResponse.INFO;
        msg.payload = protocolLine[5..$-2];
    }
    else if (protocolLine.startsWith(ERR))
    {
        msg.type = NatsResponse.ERR;
        msg.payload = protocolLine[5..$-2];
    }
    else
    {
        version(Have_vibe_core)
        {
            import vibe.core.log;
            logDebug("protocolLine: %s", protocolLine);
        }
        throw new NatsProtocolException("Expected start of a NATS response token.");			
    }
    return consumed;
}


unittest {
    enum test_msg = "MSG notices 1 12\r\nHello world!\r\n".representation;
    enum test_msg_w_reply = "MSG notices 12 reply 29\r\nHello world - please respond!\r\n".representation;
    enum two_messages = test_msg ~ test_msg_w_reply;
    enum info = `INFO {"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`.representation
        ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`.representation
        ~ "\r\n".representation;

    void idiomatic_d()
    { 
        Msg msg1, msg2;
        size_t consumed;
        
        consumed = parseNats(msg1, test_msg);
        assert(consumed == 18);
        assert(msg1.type == NatsResponse.MSG);
        assert(msg1.subject == "notices");
        assert(msg1.sid == 1);
        assert(test_msg[consumed..$] == "Hello world!\r\n");

        consumed = parseNats(msg2, test_msg_w_reply);
        assert(consumed == 25);
        assert(msg2.type == NatsResponse.MSG_REPLY);
        assert(msg2.subject == "notices");
        assert(msg2.replySubject == "reply");
        assert(msg2.sid == 12);	
        assert(test_msg_w_reply[consumed..$] == "Hello world - please respond!\r\n");

        msg1 = Msg.init;
        msg2 = Msg.init;
        consumed = parseNats(msg1, two_messages);
        assert(msg1.subject == "notices");
        assert(msg1.length == 12);
        consumed += msg1.length;
        consumed += parseNats(msg2, two_messages[consumed .. $]);
        assert(msg2.type == NatsResponse.FRAGMENT);
        msg2 = Msg.init;
        consumed += parseNats(msg2, two_messages[consumed .. $]);
        assert(msg2.type == NatsResponse.MSG_REPLY);
        assert(msg2.replySubject == "reply");
        assert(msg2.length == 29);       

        msg1 = Msg.init;
        consumed = parseNats(msg1, test_msg[0..17]);
        assert(msg1.type == NatsResponse.FRAGMENT);
        assert(consumed == 0);

        msg1 = Msg.init;
        consumed = parseNats(msg1, info[0..65]);
        assert(msg1.type == NatsResponse.FRAGMENT);
        assert(consumed == 0);

        msg1 = Msg.init;
        consumed = parseNats(msg1, info);
        assert(msg1.type == NatsResponse.INFO);
        assert(msg1.payloadAsString == 
            `{"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`
            ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`);
    }
    idiomatic_d();

    import std.datetime.stopwatch: benchmark;
    import std.stdio: writeln;

    // auto results = benchmark!(idiomatic_d, idiomatic_d_new, go_port)(1_000);
    auto results = benchmark!(idiomatic_d)(1_000);

    writeln("7000x idiomatic_d parser: ", results[0]);
    // writeln("7000x idiomatic_d_new parser: ", results[1]);
    // writeln("7000x go_natsparser_port: ", results[2]);	

}
