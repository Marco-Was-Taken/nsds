package com.polimi.nsds.akka.ex4;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Stateful echo server actor for Exercise 4 that alternates between an <b>awake</b> behavior
 * and a <b>sleeping</b> behavior using {@code getContext().become(...)} and {@code stash()} /
 * {@code unstashAll()}.
 * <p>
 * <b>Key Akka Mechanisms Used:</b>
 * <ul>
 *   <li><b>Dynamic Behavior ({@code getContext().become(...)} — Lecture 1, Slides 24–25):</b>
 *       The server starts in its normal awake behavior ({@link #createReceive()}), where it echoes
 *       every {@link TextMessage} back to {@code sender()} and transitions to {@link #onSleepMode()}
 *       upon receiving a {@link SleepMessage}.</li>
 *   <li><b>Message Stashing ({@link AbstractActorWithStash} — Lecture 2, Slide 9):</b>
 *       While in {@link #onSleepMode()}, the server cannot process {@link TextMessage}s immediately,
 *       so it calls {@code stash()} to set them aside in an internal buffer (preserving their original
 *       {@code sender()} reference). When a {@link WakeupMessage} arrives, the server calls
 *       {@code unstashAll()} to prepend all stashed messages back to the front of its mailbox in FIFO
 *       order and switches back to {@link #createReceive()}.</li>
 * </ul>
 * </p>
 */
public class ServerActor extends AbstractActorWithStash {

    /**
     * Initial (awake) behavior: echoes {@link TextMessage}s immediately and switches to sleep mode
     * upon receiving a {@link SleepMessage}.
     *
     * @return the awake {@link Receive} behavior
     */
    @Override
    public Receive createReceive() {
        // Awake mode: process TextMessage and SleepMessage; any WakeupMessage in awake mode is ignored
        return receiveBuilder()
                .match(TextMessage.class, this::onTextMessage)
                .match(SleepMessage.class, this::onSleepMessage)
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
     * Echoes the received {@link TextMessage} back to the client that sent it.
     *
     * @param message the text message to echo back
     */
    private void onTextMessage(TextMessage message) {
        // Reply to the sender of this message (ClientActor)
        sender().tell(message, ActorRef.noSender());
    }

    /**
     * Transitions the actor from awake mode to sleep mode upon receiving a {@link SleepMessage}.
     *
     * @param message the sleep control message
     */
    private void onSleepMessage(SleepMessage message) {
        // Switch active message-handling behavior to onSleepMode()
        getContext().become(onSleepMode());
    }

    /**
     * Sleeping behavior: stashes incoming {@link TextMessage}s for later processing and waits
     * for a {@link WakeupMessage} to restore normal operation.
     *
     * @return the sleeping {@link Receive} behavior
     */
    private Receive onSleepMode() {
        // Sleep mode: postpone TextMessages via stash() and listen for WakeupMessage
        return receiveBuilder()
                .match(TextMessage.class, this::onStashMessage)
                .match(WakeupMessage.class, this::onWakeMessage)
                .build();
    }

    /**
     * Postpones a {@link TextMessage} received while the server is sleeping.
     *
     * @param message the text message to stash
     */
    private void onStashMessage(TextMessage message) {
        // Save the current message (along with its sender() metadata) in the actor's stash buffer
        stash();
    }

    /**
     * Wakes the server up: prepends all stashed {@link TextMessage}s back to the front of the
     * mailbox in FIFO order and reverts to the awake behavior.
     *
     * @param message the wakeup control message
     */
    private void onWakeMessage(WakeupMessage message) {
        // Extract all stashed messages in FIFO order and put them at the front of the mailbox
        unstashAll();
        // Revert to the initial awake behavior so the unstashed TextMessages are echoed immediately
        getContext().become(createReceive());
    }
}
