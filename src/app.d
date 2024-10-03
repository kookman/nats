import vibe.core.core;
import vibe.core.log;
import core.time; 

import nats.client;

// minimise overhead by turning off parallel GC. We don't generate much garbage anyway
extern(C) __gshared string[] rt_options = ["gcopt=parallel:0"];

Nats natsConn;

void main(string[] args)
{
    import std.functional: toDelegate;

    setLogFormat(FileLogger.Format.threadTime, FileLogger.Format.threadTime);
    natsConn = new Nats("nats://127.0.0.1:4222", "nats:testClient");

    logInfo("Starting Nats client version: %s", Nats.natsClientVersion);
    natsConn.connect();

    runTask(() nothrow {
            // use giant try/catch bazooka to ensure task is nothrow
            // this is needed to avoid deprecations in vibe-core 1.18+
            try {
                natsConn.waitForConnection();
                natsConn.subscribe(">", toDelegate(&sub_all_handler));
                logInfo("Sent subscribe all.");
                auto greets = natsConn.subscribe("greetings", toDelegate(&greetings_handler));
                auto responder = natsConn.subscribe("query", toDelegate(&query_responder));
                auto shutdown = natsConn.subscribe("exit", toDelegate(&shutdown_handler));
                natsConn.publish("back_channel", cast(ubyte[])"This came on the back channel...");
                natsConn.publish("greetings", cast(ubyte[])"Hello to all greetings subscribers!");		
                sleep(50.msecs);
                natsConn.unsubscribe(greets);
                natsConn.publishRequest("query", cast(ubyte[])"721256", toDelegate(&response_handler));
                sleep(5000.msecs);
                natsConn.publish("greetings", cast(ubyte[])"Still there?");
                natsConn.publishRequest("query", cast(ubyte[])"722739", toDelegate(&response_handler));
            }
            catch (Exception e) {
                logError("Exception thrown in test messages: (%s)", e.msg);
            }
        });

    runApplication();
}

void shutdown_handler(scope Msg msg) @safe nothrow
{
    logInfo("Shutdown msg received. Disconnecting and shutting down.");
    natsConn.close();
    exitEventLoop();
}

void sub_all_handler(scope Msg msg) @safe nothrow
{
    logInfo("sub_all_handler--> msg subject: %s, msg payload: %s", msg.subject, msg.payloadAsString);
}

void greetings_handler(scope Msg msg) @safe nothrow
{
    logInfo("greetings_handler--> %s", msg.payloadAsString);
}

void query_responder(scope Msg msg) @safe nothrow
{
    import std.conv: to;
    import std.format: sformat;
    import std.random: uniform;

    void responder(string reply, int data, Nats conn) @safe nothrow
    {
        try {
            char[80] buffer;
            int delay = uniform(0, 200);
            logInfo("responder %d --> starting %d ms long calc...", data, delay);  
            sleep(delay.msecs); 
            logInfo("responder %d --> done. Sending reponse to %s", data, reply);
            conn.publish(reply, buffer.sformat!"Answering query number %d"(data));
        }
        catch (Exception e) {
            logError("Responder failed with exception: (%s)", e.msg);
        }
    }

    // note: we runTask to avoid blocking the listener task
    // so we must extract the msg contents we need (since it is scoped)
    auto replySubject = msg.replySubject.idup;
    try {
        int id = to!int(msg.payloadAsString);
        runTask(&responder, replySubject, id, natsConn);
    }
    catch (Exception e) {
        logWarn("Failed to deserialize request message: (%s)", msg.payloadAsString);
    }
}

void response_handler(scope Msg msg) @safe nothrow
{
    logInfo("response_handler--> Got the request response on %s, message: %s", 
        msg.subject, msg.payloadAsString);
}
