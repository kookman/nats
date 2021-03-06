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


size_t parseNats(scope const(ubyte)[] response, out Msg msg) @safe
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


size_t parseNatsNew(scope const(ubyte)[] response, out Msg msg) @trusted
{
    import std.algorithm.searching: find, findSplitBefore;
    import std.conv: to;

    string[5] token;
    bool wholeLine;
    uint tokenCount;
    ulong tokenLength;

    size_t consumed = 0;
    auto remaining = response;
    loop: do
    {
        auto tokenSplitter = remaining.find(' ',CRLF);
        if (!tokenSplitter[1])
        {
            msg.type = NatsResponse.FRAGMENT;
            return consumed;		
        }
        tokenLength = remaining.length - tokenSplitter[0].length;
        token[tokenCount] = remaining[0..tokenLength].assumeUTF.assumeUnique;
        remaining = tokenSplitter[0][tokenSplitter[1]..$];
        tokenCount++;
        if (tokenSplitter[1] == 2)
        {
            wholeLine = true;
        }
        final switch (msg.type)
        {
            case NatsResponse.FRAGMENT:
                switch (token[0])
                {
                    case "MSG":
                        msg.type = NatsResponse.MSG;
                        break;
                    case "PONG":
                        msg.type = NatsResponse.PONG;
                        break;
                    case "PING":
                        msg.type = NatsResponse.PING;
                        break;
                    case "+OK":
                        msg.type = NatsResponse.OK;
                        break;
                    case "INFO":
                        msg.type = NatsResponse.INFO;
                        break;			
                    case "-ERR":
                        msg.type = NatsResponse.ERR;
                        break;
                    default:
                        throw new NatsProtocolException("Expected a NATS response token.");
                }
                continue loop;
            case NatsResponse.INFO:
            case NatsResponse.ERR:
                auto payload = response.findSplitBefore(CRLF);
                if (payload)
                {
                    msg.payload = payload[0][5..$];
                } 
                else
                    msg.type = NatsResponse.FRAGMENT;
                break loop;
            case NatsResponse.MSG:
            case NatsResponse.MSG_REPLY:
                continue loop;
            case NatsResponse.PONG:
            case NatsResponse.PING:
            case NatsResponse.OK:
                break loop;
        }
    } while (!wholeLine && tokenCount < 5 && msg.type != NatsResponse.FRAGMENT);

    if (msg.type == NatsResponse.MSG)
    {
        msg.subject = token[1];
        msg.sid = token[2].to!uint;
        if (tokenCount == 4)
            msg.length = token[3].to!uint;				
        else
        {
            msg.type = NatsResponse.MSG_REPLY;
            msg.replySubject = token[3];
            msg.length = token[4].to!uint;								
        }
        if (msg.length + 2 <= remaining.length)
        {
            msg.payload = remaining[0..msg.length];
            consumed += msg.length + 2;
        }
    } 
    return consumed;
}


void processMsgArgs(scope const(ubyte)[] args, out Msg msg) @trusted
{
    MsgField field;
    ubyte    b;
    size_t   start;

    import std.ascii: isAlpha;

    for (size_t i; i < args.length; i++) {
        b = args[i];
        final switch (field) {
            case MsgField.SUBJECT:
                switch (b) {
                    case ' ':
                        msg.subject = assumeUTF(args[0..i]);
                        msg.type = NatsResponse.MSG;
                        field = MsgField.SID;
                        continue;
                    default:
                        continue;
                }
            case MsgField.SID:
                switch (b) {
                    case '0': .. case '9':
                        msg.sid *= 10;
                        msg.sid += (b - 48);
                        continue;
                    default:
                        start = i+1;
                        field = isAlpha(cast(char)args[start]) ? MsgField.REPLY : MsgField.LENGTH;
                        continue;
                }
            case MsgField.REPLY:
                switch (b) {
                    case ' ':
                        msg.replySubject = assumeUTF(args[start..i]);
                        msg.type = NatsResponse.MSG_REPLY;
                        field = MsgField.LENGTH;
                        continue;
                    default:
                        continue;
                }
            case MsgField.LENGTH:
                switch (b) {
                    case '0': .. case '9':
                        msg.length *= 10;
                        msg.length += (b - 48);
                        continue;
                    default:
                        break;
                } 
        }        
    }
}



size_t parse(scope const(ubyte)[] response, out Msg msg) @trusted
{
    CmdState cmd;
    ubyte b;
    uint start;
    uint drop;

    msgloop:
    for (uint i; i < response.length; i++)
    {
        b = response[i];
        final switch (cmd)
        {
            case CmdState.OP_START:
                switch (b) {
                    case 'M':
                    case 'm':
                        cmd = CmdState.OP_M;
                        continue;
                    case 'P':
                    case 'p':
                        cmd = CmdState.OP_P;
                        continue;
                    case '+':
                        cmd = CmdState.OP_PLUS;
                        continue;
                    case '-':
                        cmd = CmdState.OP_MINUS;
                        continue;
                    case 'I':
                    case 'i':
                        cmd = CmdState.OP_I;
                        continue;
                    default:
                        throw new NatsProtocolException("Expected start of a NATS response token.");
                }
            case CmdState.OP_M:
                switch (b) {
                    case 'S':
                    case 's':
                        cmd = CmdState.OP_MS;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting MSG token.");
                }
            case CmdState.OP_MS:
                switch (b) {
                    case 'G':
                    case 'g':
                        cmd = CmdState.OP_MSG;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting MSG token.");
                }
            case CmdState.OP_MSG:
                switch (b) {
                    case ' ':
                    case '\t':
                        cmd = CmdState.OP_MSG_SPC;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting whitespace after MSG token.");
                }
            case CmdState.OP_MSG_SPC:
                switch (b) {
                    case ' ':
                    case '\t':
                        continue;
                    default:
                        cmd = CmdState.MSG_ARG;
                        start = i;
                        continue;
                }
            case CmdState.MSG_ARG:
                switch (b) {
                    case '\r':
                        drop = 1;
                        continue;
                    case '\n':
                        processMsgArgs(response[start..i-drop], msg);
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.MSG_PAYLOAD;
                        continue;
                    default:
                        continue;
                }
            case CmdState.MSG_PAYLOAD:
                break msgloop;

            case CmdState.MSG_END:
                switch (b) {
                    case '\n':
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.OP_START;
                        break msgloop;						
                    default:
                        continue;
                }
            case CmdState.OP_PLUS:
                switch (b) {
                    case 'O':
                    case 'o':
                        cmd = CmdState.OP_PLUS_O;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting +OK token.");
                }
            case CmdState.OP_PLUS_O:
                switch (b) {
                    case 'K':
                    case 'k':
                        cmd = CmdState.OP_PLUS_OK;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting +OK token.");
                }
            case CmdState.OP_PLUS_OK:
                switch (b) {
                    case '\r':
                        continue;
                    case '\n':
                        msg.type = NatsResponse.OK;
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.OP_START;
                        break msgloop;
                    default:
                        throw new NatsProtocolException("Error after +OK token.");
                }
            case CmdState.OP_P:
                switch (b) {
                    case 'I':
                    case 'i':
                        cmd = CmdState.OP_PI;
                        continue;
                    case 'O':
                    case 'o':
                        cmd = CmdState.OP_PO;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting PING or PONG token.");
                }
            case CmdState.OP_PO:
                switch (b) {
                    case 'N':
                    case 'n':
                        cmd = CmdState.OP_PON;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting PONG token.");
                }
            case CmdState.OP_PON:
                switch (b) {
                    case 'G':
                    case 'g':
                        cmd = CmdState.OP_PONG;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting PONG token.");
                }
            case CmdState.OP_PONG:
                switch (b) {
                    case '\r':
                        continue;
                    case '\n':
                        msg.type = NatsResponse.PONG;
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.OP_START;
                        break msgloop;
                    default:
                        throw new NatsProtocolException("Error after PONG token.");
                }
            case CmdState.OP_PI:
                switch (b) {
                    case 'N':
                    case 'n':
                        cmd = CmdState.OP_PIN;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting PING token.");
                }
            case CmdState.OP_PIN:
                switch (b) {
                    case 'G':
                    case 'g':
                        cmd = CmdState.OP_PING;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting PING token.");
                }
            case CmdState.OP_PING:
                switch (b) {
                    case '\r':
                        continue;
                    case '\n':
                        msg.type = NatsResponse.PING;
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.OP_START;
                        break msgloop;
                    default:
                        throw new NatsProtocolException("Error after PING token.");
                }
            case CmdState.OP_MINUS:
                switch (b) {
                    case 'E':
                    case 'e':
                        cmd = CmdState.OP_MINUS_E;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting -ERR token.");
                }
            case CmdState.OP_MINUS_E:
                switch (b) {
                    case 'R':
                    case 'r':
                        cmd = CmdState.OP_MINUS_ER;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting -ERR token.");
                }
            case CmdState.OP_MINUS_ER:
                switch (b) {
                    case 'R':
                    case 'r':
                        cmd = CmdState.OP_MINUS_ERR;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting -ERR token.");
                }
            case CmdState.OP_MINUS_ERR:
                switch (b) {
                    case ' ':
                    case '\t':
                        cmd = CmdState.OP_MINUS_ERR_SPC;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting -ERR token.");
                }
            case CmdState.OP_MINUS_ERR_SPC:
                switch (b) {
                    case ' ':
                    case '\t':
                        continue;
                    default:
                        cmd = CmdState.MINUS_ERR_ARG;
                        start = i;
                        continue;
                }
            case CmdState.MINUS_ERR_ARG:
                switch (b) {
                    case '\r':
                        drop = 1;
                        continue;
                    case '\n':
                        msg.type = NatsResponse.ERR;
                        msg.payload = response[start..i-drop];
                        // processMinusErrArgs returns the number of bytes it reads from buffer
                        //i += processMinusErrArgs(_buffer[start..i-drop]);
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.OP_START;
                        break msgloop;
                    default:
                        continue;
                }
            case CmdState.OP_I:
                switch (b) {
                    case 'N':
                    case 'n':
                        cmd = CmdState.OP_IN;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting INFO token.");
                }
            case CmdState.OP_IN:
                switch (b) {
                    case 'F':
                    case 'f':
                        cmd = CmdState.OP_INF;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting INFO token.");
                }
            case CmdState.OP_INF:
                switch (b) {
                    case 'O':
                    case 'o':
                        cmd = CmdState.OP_INFO;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting INFO token.");
                }
            case CmdState.OP_INFO:
                switch (b) {
                    case ' ':
                    case '\t':
                        cmd = CmdState.OP_INFO_SPC;
                        continue;
                    default:
                        throw new NatsProtocolException("Was expecting INFO token.");
                }
            case CmdState.OP_INFO_SPC:
                switch (b) {
                    case ' ':
                    case '\t':
                        continue;
                    default:
                        cmd = CmdState.INFO_ARG;
                        start = i;
                        continue;
                }
            case CmdState.INFO_ARG:
                switch (b) {
                    case '\r':
                        drop = 1;
                        continue;
                    case '\n':
                        msg.type = NatsResponse.INFO;
                        msg.payload = response[start..i-drop];
                        start = i+1;
                        drop = 0;
                        cmd = CmdState.OP_START;
                        break msgloop;
                    default:
                        continue;
                }
        }
    }
    if (cmd != CmdState.OP_START)
    {
        // anything else means we have ended on a fragmented Nats command
        msg.type = NatsResponse.FRAGMENT;
    }
    return response.length - start;
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
        
        consumed = parseNats(test_msg, msg1);
        assert(consumed == 18);
        assert(msg1.type == NatsResponse.MSG);
        assert(msg1.subject == "notices");
        assert(msg1.sid == 1);
        assert(test_msg[consumed..$] == "Hello world!\r\n");

        consumed = parseNats(test_msg_w_reply, msg2);
        assert(consumed == 25);
        assert(msg2.type == NatsResponse.MSG_REPLY);
        assert(msg2.subject == "notices");
        assert(msg2.replySubject == "reply");
        assert(msg2.sid == 12);	
        assert(test_msg_w_reply[consumed..$] == "Hello world - please respond!\r\n");

        msg1 = Msg.init;
        msg2 = Msg.init;
        consumed = parseNats(two_messages, msg1);
        assert(msg1.subject == "notices");
        assert(msg1.length == 12);
        consumed += msg1.length;
        consumed += parseNats(two_messages[consumed .. $], msg2);
        assert(msg2.type == NatsResponse.FRAGMENT);
        msg2 = Msg.init;
        consumed += parseNats(two_messages[consumed .. $], msg2);
        assert(msg2.type == NatsResponse.MSG_REPLY);
        assert(msg2.replySubject == "reply");
        assert(msg2.length == 29);       

        msg1 = Msg.init;
        consumed = parseNats(test_msg[0..17], msg1);
        assert(msg1.type == NatsResponse.FRAGMENT);
        assert(consumed == 0);

        msg1 = Msg.init;
        consumed = parseNats(info[0..65], msg1);
        assert(msg1.type == NatsResponse.FRAGMENT);
        assert(consumed == 0);

        msg1 = Msg.init;
        consumed = parseNats(info, msg1);
        assert(msg1.type == NatsResponse.INFO);
        assert(msg1.payloadAsString == 
            `{"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`
            ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`);
    }
    idiomatic_d();

    // void idiomatic_d_new()
    // { 
    //     Msg msg1, msg2;
        
    //     msg1 = parseNatsNew(test_msg);
    //     assert(msg1.type == NatsResponse.MSG);
    //     assert(msg1.subject == "notices");
    //     assert(msg1.payload == "Hello world!");
    //     assert(msg1.sid == 1);

    //     msg2 = parseNatsNew(test_msg_w_reply);
    //     assert(msg2.type == NatsResponse.MSG_REPLY);
    //     assert(msg2.subject == "notices");
    //     assert(msg2.payload == "Hello world - please respond!");
    //     assert(msg2.replySubject == "reply");
    //     assert(msg2.sid == 12);	

    //     msg1 = parseNatsNew(two_messages);
    //     assert(msg1.payload == "Hello world!");
    //     assert(msg1.sid == 1);

    //     ubyte[] remaining = two_messages[msg1.consumed..$];
    //     msg2 = parseNatsNew(remaining);
    //     assert(msg2.payload == "Hello world - please respond!");
    //     assert(msg2.sid == 12);	

    //     msg1 = parseNatsNew(test_msg[0..12]);
    //     assert(msg1.type == NatsResponse.FRAGMENT);
    //     assert(msg1.consumed == 0);

    //     msg1 = parseNatsNew(info[0..65]);
    //     assert(msg1.type == NatsResponse.FRAGMENT);
    //     assert(msg1.consumed == 0);

    //     msg1 = parseNatsNew(info);
    //     assert(msg1.type == NatsResponse.INFO);
    //     assert(msg1.payloadAsString == 
    //         `{"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`
    //         ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`);
    // }

    // void go_port()
    // {
    //     Msg msg1, msg2;
        
    //     msg1 = parse(test_msg);
    //     assert(msg1.type == NatsResponse.MSG);
    //     assert(msg1.subject == "notices");
    //     assert(msg1.payload == "Hello world!");
    //     assert(msg1.sid == 1);

    //     msg2 = parse(test_msg_w_reply);
    //     assert(msg2.type == NatsResponse.MSG_REPLY);
    //     assert(msg2.subject == "notices");
    //     assert(msg2.payload == "Hello world - please respond!");
    //     assert(msg2.replySubject == "reply");
    //     assert(msg2.sid == 12);	

    //     msg1 = parse(two_messages);
    //     assert(msg1.payload == "Hello world!");
    //     assert(msg1.sid == 1);

    //     ubyte[] remaining = two_messages[msg1.consumed..$];
    //     msg2 = parse(remaining);
    //     assert(msg2.payload == "Hello world - please respond!");
    //     assert(msg2.sid == 12);	

    //     msg1 = parse(test_msg[0..12]);
    //     assert(msg1.type == NatsResponse.FRAGMENT);
    //     assert(msg1.consumed == 0);

    //     msg1 = parse(info[0..65]);
    //     assert(msg1.type == NatsResponse.FRAGMENT);
    //     assert(msg1.consumed == 0);

    //     msg1 = parse(info);
    //     assert(msg1.type == NatsResponse.INFO);
    //     assert(msg1.payloadAsString == 
    //         `{"server_id":"p5YHW98yUXPd3BTRHoBNAE","version":"1.4.1","proto":1,`
    //         ~ `"go":"go1.11.5","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":12}`);
    // }

    import std.datetime.stopwatch: benchmark;
    import std.stdio: writeln;

    // auto results = benchmark!(idiomatic_d, idiomatic_d_new, go_port)(1_000);
    auto results = benchmark!(idiomatic_d)(1_000);

    writeln("7000x idiomatic_d parser: ", results[0]);
    // writeln("7000x idiomatic_d_new parser: ", results[1]);
    // writeln("7000x go_natsparser_port: ", results[2]);	

}


enum CmdState {
    OP_START,
    OP_PLUS,
    OP_PLUS_O,
    OP_PLUS_OK,
    OP_MINUS,
    OP_MINUS_E,
    OP_MINUS_ER,
    OP_MINUS_ERR,
    OP_MINUS_ERR_SPC,
    MINUS_ERR_ARG,
    OP_M,
    OP_MS,
    OP_MSG,
    OP_MSG_SPC,
    MSG_ARG,
    MSG_PAYLOAD,
    MSG_END,
    OP_P,
    OP_PI,
    OP_PIN,
    OP_PING,
    OP_PO,
    OP_PON,
    OP_PONG,
    OP_I,
    OP_IN,
    OP_INF,
    OP_INFO,
    OP_INFO_SPC,
    INFO_ARG
}

enum MsgField {
    SUBJECT,
    SID,
    REPLY,
    LENGTH
}
