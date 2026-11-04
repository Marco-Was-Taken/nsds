package com.polimi.nsds.akka.ex3;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Client actor for Exercise 3 that waits for the reply to each {@link GetMsg} without blocking,
 * using the workaround described in the slide "Motivation for the Ask Pattern":
 * <ul>
 *   <li>after sending a GetMsg, the actor changes its behavior with
 *       {@code getContext().become(...)} to a state that matches only the expected
 *       {@link ReplyMsg} and stashes all other messages;</li>
 *   <li>upon receiving the ReplyMsg, it prints it, unstashes all stashed messages and reverts
 *       to the normal behavior.</li>
 * </ul>
 * This guarantees that requests are issued one at a time: a request that arrives while a
 * reply is pending is processed only after that reply has been displayed.
 * The main program sends PutMsg/GetMsg commands to this client, which forwards them to the
 * server with itself as sender, so the server's reply comes back to this actor's mailbox.
 */
public class ClientActor extends AbstractActorWithStash {

    // Reference to the server actor, received at creation time
    private final ActorRef server;

    /**
     * Creates the client bound to the given server.
     *
     * @param server ActorRef of the ServerActor
     */
    public ClientActor(ActorRef server) {
        // Store the server reference
        this.server = server;
    }

    /**
     * The initial behavior is the "idle" one, in which commands are accepted.
     *
     * @return the idle Receive
     */
    @Override
    public Receive createReceive() {
        // Start in the idle state (same structure as the AlarmActor example in the slides)
        return idle();
    }

    /**
     * Behavior used when no reply is pending: PutMsg and GetMsg are forwarded to the server.
     *
     * @return the idle Receive
     */
    private final Receive idle() {
        // Commands accepted while idle
        return receiveBuilder()
                .match(PutMsg.class, this::onPut)
                .match(GetMsg.class, this::onGet)
                .build();
    }

    /**
     * Behavior used while waiting for the reply to a GetMsg: the ReplyMsg is handled,
     * every other message is stashed.
     *
     * @return the waiting Receive
     */
    private final Receive waitingForReply() {
        // Only the expected reply is processed, everything else is postponed
        return receiveBuilder()
                .match(ReplyMsg.class, this::onReply)
                .matchAny(msg -> stash())
                .build();
    }

    /**
     * Forwards a PutMsg to the server. No reply is expected, so the behavior does not change.
     *
     * @param msg the contact to store
     */
    void onPut(PutMsg msg) {
        // Log and forward the request, with this actor as sender
        System.out.println("[Client] Put " + msg.getName() + " -> " + msg.getEmail());
        server.tell(msg, self());
    }

    /**
     * Forwards a GetMsg to the server and switches to the waiting behavior.
     *
     * @param msg the lookup request
     */
    void onGet(GetMsg msg) {
        // Log and forward the request, with this actor as sender so that the reply comes back here
        System.out.println("[Client] Get " + msg.getName());
        server.tell(msg, self());
        // Wait for the reply: from now on only ReplyMsg is processed
        getContext().become(waitingForReply());
    }

    /**
     * Displays the reply, restores the stashed messages and goes back to the idle behavior.
     *
     * @param reply the reply received from the server
     */
    void onReply(ReplyMsg reply) {
        // Display the content of the reply on the console
        if (reply.isFound()) {
            System.out.println("[Client] REPLY: " + reply.getName() + " -> " + reply.getEmail());
        } else {
            System.out.println("[Client] REPLY: " + reply.getStatusMessage());
        }
        // Re-enqueue the commands received while waiting, in their original order
        unstashAll();
        // Revert to the normal behavior
        getContext().become(idle());
    }

    /**
     * Factory method returning the Props used to create this actor.
     *
     * @param server ActorRef of the ServerActor passed to the constructor
     * @return Props for ClientActor
     */
    static Props props(ActorRef server) {
        // Props.create(Class, constructor arguments...)
        return Props.create(ClientActor.class, server);
    }
}
