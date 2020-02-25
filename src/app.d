import vibe.core.core;
import vibe.core.log;

import nats.client;

void main(string[] args)
{
    import std.functional: toDelegate;

    auto nats = new Nats("nats://127.0.0.1:4222", "nats:testClient");

    logInfo("Starting Nats client version: %s", Nats.natsClientVersion);
    nats.connect();

    runTask({
                import core.time; 

                sleep(50.msecs);
                nats.subscribe(">", toDelegate(&sub_all_handler));
                logInfo("Sent subscribe all.");
                auto greets = nats.subscribe("greetings", toDelegate(&greetings_handler));
                nats.publish("back_channel", cast(ubyte[])"This came on the back channel...");
                nats.publish("greetings", cast(ubyte[])"Hello to all greetings subscribers!");		
                sleep(50.msecs);
                nats.unsubscribe(greets);
                nats.publish("greetings", cast(ubyte[])"Still there?");
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

