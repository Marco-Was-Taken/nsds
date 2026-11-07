package com.polimi.nsds.akka.ex3;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Alternative client actor for Exercise 3 based on the <b>Ask Pattern</b> (slide "Ask Pattern").
 * <p>
 * {@code Patterns.ask(receiver, msg, timeout)} sends the message and returns a future that
 * will contain the reply; the server replies as usual with {@code sender().tell(reply, self())}
 * (the sender is a temporary actor that completes the future). The client then blocks on the
 * future with {@code Await.result(future, timeout)}, obtaining a synchronous behavior.
 * </p>
 * <p>
 * Note: blocking inside an actor keeps one dispatcher thread busy until the reply arrives
 * (or the timeout expires). It is acceptable for this exercise, but the non-blocking
 * become/stash solution in {@link ClientActor} scales better.
 * </p>
 */
public class ClientAskActor extends AbstractActor {

    // Timeout (in milliseconds) passed to Patterns.ask(receiver, msg, timeout)
    private static final long TIMEOUT_MS = 5000;
    // Timeout Duration passed to Await.result(future, timeout)
    private static final Duration timeout = Duration.create(5, SECONDS);

    // Reference to the server actor, received at creation time
    private final ActorRef server;

    /**
     * Creates the client bound to the given server.
     *
     * @param server ActorRef of the ServerActor
     */
    public ClientAskActor(ActorRef server) {
        // Store the server reference
        this.server = server;
    }

    /**
     * Defines the match clauses for the commands received from the main program.
     *
     * @return the Receive object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // PutMsg is fire-and-forget, GetMsg uses the Ask pattern
        return receiveBuilder()
                .match(PutMsg.class, this::onPut)
                .match(GetMsg.class, this::onGet)
                .build();
    }

    /**
     * Forwards a PutMsg to the server with tell (no reply expected).
     *
     * @param msg the contact to store
     */
    void onPut(PutMsg msg) {
        // Log and forward the request
        System.out.println("[AskClient] Put " + msg.getName() + " -> " + msg.getEmail());
        server.tell(msg, self());
    }

    /**
     * Sends a GetMsg with the Ask pattern, waits for the reply and displays it.
     *
     * @param msg the lookup request
     * @throws Exception if the reply does not arrive within the timeout or the wait is interrupted
     */
    void onGet(GetMsg msg) throws Exception {
        // Log the request
        System.out.println("[AskClient] Get " + msg.getName());
        // Send the request and obtain a future that will contain the reply
        final Future<Object> future = Patterns.ask(server, msg, TIMEOUT_MS);
        // Block until the reply is available (synchronous behavior: Await.result(future, timeout))
        final ReplyMsg reply = (ReplyMsg) Await.result(future, timeout);
        // Display the content of the reply on the console
        if (reply.isFound()) {
            System.out.println("[AskClient] REPLY: " + reply.getName() + " -> " + reply.getEmail());
        } else {
            System.out.println("[AskClient] REPLY: " + reply.getStatusMessage());
        }
    }

    /**
     * Factory method returning the Props used to create this actor.
     *
     * @param server ActorRef of the ServerActor passed to the constructor
     * @return Props for ClientAskActor
     */
    static Props props(ActorRef server) {
        // Props.create(Class, constructor arguments...)
        return Props.create(ClientAskActor.class, server);
    }
}
