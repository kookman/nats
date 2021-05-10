import vibe.core.core;
import vibe.core.log;
import core.time; 

import nats.client;

Nats natsConn;

void main(string[] args)
{
    import std.functional: toDelegate;

    natsConn = new Nats("nats://127.0.0.1:4222", "nats:testClient");

    logInfo("Starting Nats client version: %s", Nats.natsClientVersion);
    natsConn.connect();

    runTask({
                sleep(50.msecs);
                natsConn.subscribe(">", toDelegate(&sub_all_handler));
                logInfo("Sent subscribe all.");
                auto greets = natsConn.subscribe("greetings", toDelegate(&greetings_handler));
                auto responder = natsConn.subscribe("MoL", toDelegate(&responder));
                natsConn.publish("back_channel", cast(ubyte[])"This came on the back channel...");
                natsConn.publish("greetings", cast(ubyte[])"Hello to all greetings subscribers!");		
                sleep(50.msecs);
                natsConn.unsubscribe(greets);
                natsConn.publishRequest("MoL", cast(ubyte[])"What is the meaning of life?", toDelegate(&response_handler));
                sleep(5000.msecs);
                natsConn.publish("greetings", cast(ubyte[])"Still there?");
                natsConn.publishRequest("MoL", cast(ubyte[])"Are you sure its 42?", toDelegate(&response_handler));
            });

    runApplication();
}


void sub_all_handler(scope Msg msg) @safe
{
    logInfo("sub_all_handler--> msg subject: %s, msg payload: %s", msg.subject, msg.payloadAsString);
}

void greetings_handler(scope Msg msg) @safe
{
    logInfo("greetings_handler--> %s", msg.payloadAsString);
}

void responder(scope Msg msg) @safe
{
    import std.string: representation;
    import std.exception: assumeUnique;

    logInfo("responder--> starting long calc...");  
    sleep(1.seconds);   // note: this currently blocks the listener task
    logInfo("responder--> done. Sending reponse to %s", msg.replySubject);
    auto replySubject = () @trusted { return msg.replySubject.assumeUnique; }();
    natsConn.publish(replySubject, "The answer is 42!".representation);
}

void response_handler(scope Msg msg) @safe
{
    logInfo("response_handler--> Got the request response on %s, message: %s", 
        msg.subject, msg.payloadAsString);
}
