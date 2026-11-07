package com.polimi.nsds.akka.base;

import akka.actor.AbstractActor;
import akka.actor.Props;

/**
 * Counter actor of the introductory example ("Actors basics" slide).
 * <p>
 * <b>Why no locks are needed:</b> the counter is private state of the actor and can only
 * be affected through messages. Akka processes the messages of one actor one at a time:
 * the processing of a message is atomic, "as if the entire processing of a message was
 * executed in a synchronized method of a class where all methods are synchronized".
 * Messages arriving meanwhile (even from 16 different threads) are queued in the mailbox
 * and do not interfere with the running handler. Therefore {@code ++counter}, which is a
 * non-atomic read-modify-write in plain Java, is safe here without {@code synchronized},
 * locks or {@code AtomicInteger}.
 * </p>
 */
public class CounterActor extends AbstractActor {

    // Private state: never shared, only modified while processing a message
    private int counter;

    /**
     * Creates the actor and initializes its state.
     */
    public CounterActor() {
        // Initial value of the counter
        this.counter = 0;
    }

    /**
     * Defines the receive behavior: every {@link SimpleMessage} is handled by {@link #onMessage}.
     *
     * @return the Receive object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // One match clause: SimpleMessage -> onMessage
        return receiveBuilder()
                .match(SimpleMessage.class, this::onMessage)
                .build();
    }

    /**
     * Increments the counter. Executed atomically for each message taken from the mailbox.
     *
     * @param message the received message
     */
    void onMessage(SimpleMessage message) {
        // Read-modify-write without locks: only one message at a time is processed by this actor
        ++counter;
        // Print the new value
        System.out.println("Counter increased to: " + counter);
    }

    /**
     * Factory method returning the Props used to create this actor.
     *
     * @return Props for CounterActor
     */
    static Props props() {
        // Same form as the slides
        return Props.create(CounterActor.class);
    }
}
