package com.polimi.nsds.akka.ex6;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.time.Duration;

/**
 * Stateful server actor for Exercise 6 that processes one {@link TextMessage} at a time with a
 * simulated asynchronous delay (500 ms) using Akka's {@code scheduler().scheduleOnce(...)},
 * {@code getContext().become(onBusyMode())}, and {@code stash()} / {@code unstashAll()}.
 * <p>
 * <b>Key Akka Mechanisms Used:</b>
 * <ul>
 *   <li><b>Non-Blocking Delayed Processing ({@code scheduler().scheduleOnce(...)}):</b>
 *       Instead of blocking the actor's dispatcher thread with {@code Thread.sleep(500)}, the server
 *       switches to {@link #onBusyMode()} and schedules a {@link ReadyMessage} to {@code self()} after
 *       500 ms. This keeps the actor responsive so it can stash concurrent requests arriving while busy.</li>
 *   <li><b>Busy State &amp; Stashing ({@link AbstractActorWithStash}):</b>
 *       While in {@link #onBusyMode()}, any new {@link TextMessage} arriving from the same or another
 *       client is postponed via {@code stash()}. When the scheduled {@link ReadyMessage} arrives,
 *       {@link #onReadyMessage(ReadyMessage)} calls {@code unstashAll()} to restore postponed requests
 *       to the front of the mailbox in FIFO order, switches back to {@link #createReceive()} (idle mode),
 *       and sends the completed reply to the original client ({@code message.getSender()}).</li>
 * </ul>
 * </p>
 */
public class ServerActor extends AbstractActorWithStash {

    // Stores the upper-case version of the text currently being processed
    private String name;
    // Optional reference to the current client
    private ActorRef client;

    /**
     * Initial (idle) behavior: accepts the next {@link TextMessage} and starts asynchronous processing.
     *
     * @return the idle {@link Receive} behavior
     */
    @Override
    public Receive createReceive() {
        // Idle mode: accept incoming TextMessage requests
        return receiveBuilder()
                .match(TextMessage.class, this::onTextMessage)
                .build();
    }

    /**
     * Factory method returning the {@link Props} used to instantiate {@link ServerActor}.
     *
     * @return {@link Props} for {@link ServerActor}
     */
    public static Props props() {
        return Props.create(ServerActor.class);
    }

    /**
     * Starts processing an incoming {@link TextMessage}: converts the text to upper case,
     * switches the actor to {@link #onBusyMode()}, and schedules a {@link ReadyMessage} to
     * {@code self()} after 500 ms.
     *
     * @param message the incoming text message to process
     */
    private void onTextMessage(TextMessage message) {
        // Convert payload to upper case as required by Exercise 6 ("uppercase one TextMsg at a time")
        name = message.getText().toUpperCase();
        // Switch behavior to busy mode so concurrent requests are stashed while processing
        getContext().become(onBusyMode());
        // Schedule a ReadyMessage to self() after 500 ms without blocking the dispatcher thread
        // (Lab 2, Slide 4: getContext().system().scheduler().scheduleOnce(delay, recipient, msg, dispatcher, sender))
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.ofMillis(500),
                self(),
                new ReadyMessage(getSender(), name),
                getContext().dispatcher(),
                self()
        );
    }

    /**
     * Busy behavior active while a 500 ms processing timer is pending: stashes any new
     * {@link TextMessage}s and waits for the scheduled {@link ReadyMessage}.
     *
     * @return the busy {@link Receive} behavior
     */
    private Receive onBusyMode() {
        // Busy mode: stash incoming TextMessages and handle ReadyMessage when processing finishes
        return receiveBuilder()
                .match(TextMessage.class, this::onStashMessage)
                .match(ReadyMessage.class, this::onReadyMessage)
                .build();
    }

    /**
     * Postpones a {@link TextMessage} that arrived while the server is busy processing an earlier request.
     *
     * @param message the text message to stash
     */
    private void onStashMessage(TextMessage message) {
        // Save the message in the internal stash buffer for processing after ReadyMessage
        stash();
    }

    /**
     * Completes processing when the 500 ms timer fires: unstashes all postponed {@link TextMessage}s,
     * returns to the idle behavior ({@link #createReceive()}), and sends the uppercased reply back to
     * the client that submitted the request.
     *
     * @param message the scheduled ready notification carrying the original sender and uppercased text
     */
    private void onReadyMessage(ReadyMessage message) {
        // Prepend all stashed TextMessages back to the front of the mailbox in FIFO order
        unstashAll();
        // Return to idle mode so the next (unstashed) TextMessage can be processed
        getContext().become(createReceive());
        // Send the uppercased reply back to the original client that sent this request
        message.getSender().tell(new TextMessage(message.getMessage()), self());
    }
}
