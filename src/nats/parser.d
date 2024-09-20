module nats.parser;

/* Nats protocol parsing implementation */

import std.exception: assumeUnique;
import std.range: isInputRange, ElementType;
import std.string: assumeUTF, representation;
import std.traits: Unqual;

import nats.interface_;

package:

enum MSG = "MSG".representation;
enum HMSG = "HMSG".representation;
enum PING = "PING".representation;
enum PONG = "PONG".representation;
enum OK = "+OK".representation;
enum INFO = "INFO".representation;
enum ERR = "-ERR".representation;
enum CRLF = "\r\n".representation;
enum SPACE = " ".representation;
enum TAB = "\t".representation;


void parseNats(R)(return ref Msg msg, R protocolLine) @safe
    if (isInputRange!R && is(Unqual!(ElementType!R) == ubyte))
{
    import std.algorithm.comparison: equal;
    import std.algorithm.searching: startsWith;
    import std.algorithm.iteration: splitter;
    import std.ascii: isDigit;
    import std.conv: to;

    bool msgWithHeaders = protocolLine.startsWith(HMSG);
    if (msgWithHeaders || protocolLine.startsWith(MSG))
    {
        auto tokens = protocolLine.assumeUTF.splitter;
        tokens.popFront();
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
        if (msgWithHeaders)
        {
            msg.headersLength = tokens.front.to!uint;
            tokens.popFront();
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
        msg.payload = protocolLine[5..$];
    }
    else if (protocolLine.startsWith(ERR))
    {
        msg.type = NatsResponse.ERR;
        msg.payload = protocolLine[5..$];
    }
    else
    {
        version(Have_vibe_core)
        {
            import vibe.core.log;
            logDebug("protocolLine: %s", protocolLine);
        }
        throw new NatsProtocolException("Expected a valid NATS protocol line.");			
    }
}


unittest {
    enum test_msg = "MSG notices 1 12\r\nHello world!".representation;
    enum test_msg_w_reply = "MSG notices 12 reply 29\r\nHello world - please respond!".representation;
    enum info = `INFO {"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`.representation
        ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`.representation;

    static assert (isInputRange!(typeof(test_msg)));
    static assert (is(Unqual!(ElementType!(typeof(test_msg))) == ubyte));
    void d_parser_tests()
    { 
        Msg msg1, msg2;
        
        parseNats(msg1, test_msg);
        assert(msg1.type == NatsResponse.MSG);
        assert(msg1.subject == "notices");
        assert(msg1.sid == 1);
        assert(msg1.length == 12);

        parseNats(msg2, test_msg_w_reply);
        assert(msg2.type == NatsResponse.MSG_REPLY);
        assert(msg2.subject == "notices");
        assert(msg2.replySubject == "reply");
        assert(msg2.sid == 12);	
        assert(msg2.length == 29);

        msg1 = Msg.init;
        parseNats(msg1, info);
        assert(msg1.type == NatsResponse.INFO);
        assert(msg1.payloadAsString == 
            `{"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`
            ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`);
    }
    d_parser_tests();

    import std.datetime.stopwatch: benchmark;
    import std.stdio: writeln;

    // auto results = benchmark!(idiomatic_d, idiomatic_d_new, go_port)(1_000);
    auto results = benchmark!(d_parser_tests)(2_000);

    writeln("6000x test runs parseNats: ", results[0]);
    // writeln("5000x idiomatic_d_new parser: ", results[1]);
    // writeln("5000x go_natsparser_port: ", results[2]);	

}
