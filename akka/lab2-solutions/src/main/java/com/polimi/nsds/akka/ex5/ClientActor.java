package com.polimi.nsds.akka.ex5;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Client actor for Exercise 5 (identical to the non-blocking client of Exercise 3).
 * <p>
 * Forwards {@link PutMsg} and {@link GetMsg} commands to the supervised {@link ServerActor}.
 * Note that even when {@link ServerActor} crashes on {@code PutMsg("Fail!", ...)} and is
 * restarted by {@link SupervisorActor}, its {@link ActorRef} ({@code server}) remains valid
 * and its mailbox is preserved (Lecture 3, Slide 7), so {@link ClientActor} continues
 * communicating with {@code server} transparently without needing a new reference.
 * </p>
 */
public class ClientActor extends AbstractActorWithStash {

    // Reference to the supervised ServerActor (remains valid across server restarts!)
    private final ActorRef server;

    /**
     * Creates the client bound to the given supervised server.
     *
     * @param server {@link ActorRef} of the supervised {@link ServerActor}
     */
    public ClientActor(ActorRef server) {
        // Store the server reference
        this.server = server;
    }

    /**
     * Initial behavior: starts in the {@link #idle()} state where commands are accepted.
     *
     * @return the idle {@link Receive} behavior
     */
    @Override
    public Receive createReceive() {
        return idle();
    }

    /**
     * Idle behavior: forwards {@link PutMsg} and {@link GetMsg} to the server.
     *
     * @return the idle {@link Receive} behavior
     */
    private Receive idle() {
        return receiveBuilder()
                .match(PutMsg.class, this::onPut)
                .match(GetMsg.class, this::onGet)
                .build();
    }

    /**
     * Waiting behavior active after sending a {@link GetMsg}: processes only {@link ReplyMsg}
     * and stashes any other incoming commands until the reply arrives.
     *
     * @return the waiting {@link Receive} behavior
     */
    private Receive waitingForReply() {
        return receiveBuilder()
                .match(ReplyMsg.class, this::onReply)
                .matchAny(msg -> stash())
                .build();
    }

    /**
     * Forwards a {@link PutMsg} to the server. No reply is expected, so the behavior stays {@link #idle()}.
     *
     * @param msg the contact to store (or {@code "Fail!"} to trigger a server fault)
     */
    void onPut(PutMsg msg) {
        System.out.println("[Client] Put " + msg.getName() + " -> " + msg.getEmail());
        server.tell(msg, self());
    }

    /**
     * Forwards a {@link GetMsg} to the server and switches to {@link #waitingForReply()}.
     *
     * @param msg the lookup request
     */
    void onGet(GetMsg msg) {
        System.out.println("[Client] Get " + msg.getName());
        server.tell(msg, self());
        getContext().become(waitingForReply());
    }

    /**
     * Prints the server's {@link ReplyMsg}, unstashes postponed commands, and returns to {@link #idle()}.
     *
     * @param reply the reply received from the server
     */
    void onReply(ReplyMsg reply) {
        if (reply.isFound()) {
            System.out.println("[Client] REPLY: " + reply.getName() + " -> " + reply.getEmail());
        } else {
            System.out.println("[Client] REPLY: " + reply.getStatusMessage());
        }
        unstashAll();
        getContext().become(idle());
    }

    /**
     * Factory method returning the {@link Props} used to instantiate {@link ClientActor}.
     *
     * @param server {@link ActorRef} of the supervised {@link ServerActor}
     * @return {@link Props} for {@link ClientActor}
     */
    public static Props props(ActorRef server) {
        return Props.create(ClientActor.class, server);
    }
}
