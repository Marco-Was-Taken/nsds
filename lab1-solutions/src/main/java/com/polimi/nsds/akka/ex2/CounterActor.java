package com.polimi.nsds.akka.ex2;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;

/**
 * Counter actor whose state is never negative (Exercise 2), built on the class-based
 * solution of Exercise 1.
 * <p>
 * <b>Stash (slide "Stash"):</b> a message that cannot be processed in the current state can
 * be stashed for later processing. This requires inheriting from
 * {@link AbstractActorWithStash}:
 * <ul>
 *   <li>{@code stash()} saves the message currently being processed for later;</li>
 *   <li>{@code unstashAll()} extracts all stashed messages, in the same order in which they
 *       were added, and puts them back at the front of the mailbox, so they are processed
 *       again before any message that arrived later.</li>
 * </ul>
 * A decrement arriving when {@code counter == 0} is stashed. Every increment makes a
 * decrement possible again, so after incrementing the actor calls {@code unstashAll()}:
 * the first re-delivered decrement brings the counter back to 0, the following ones find
 * the counter at 0 and are stashed again. Stashing, state updates and unstashing all happen
 * inside the atomic processing of a single message, so no race is possible.
 * </p>
 */
public class CounterActor extends AbstractActorWithStash {

    // Private state, invariant: counter >= 0
    private int counter;

    /**
     * Creates the actor and initializes its state.
     */
    public CounterActor() {
        // Initial value of the counter
        this.counter = 0;
    }

    /**
     * Defines one match clause per message class (as in Exercise 1, solution A).
     *
     * @return the Receive object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // Increment and decrement are handled by different methods
        return receiveBuilder()
                .match(IncrementMsg.class, this::onIncrement)
                .match(DecrementMsg.class, this::onDecrement)
                .build();
    }

    /**
     * Increments the counter and re-enqueues the stashed decrements, which may now be applicable.
     *
     * @param msg the increment request
     */
    void onIncrement(IncrementMsg msg) {
        // Increment the private state
        ++counter;
        // Print the new state
        System.out.println("Counter increased to: " + counter);
        // Put all stashed decrements back at the front of the mailbox, in their original order
        unstashAll();
    }

    /**
     * Decrements the counter if it is positive, otherwise stashes the message for later.
     *
     * @param msg the decrement request
     */
    void onDecrement(DecrementMsg msg) {
        if (counter > 0) {
            // Decrementing keeps the counter non-negative
            --counter;
            // Print the new state
            System.out.println("Counter decreased to: " + counter);
        } else {
            // Decrementing now would make the counter negative: process this message later
            System.out.println("Counter is 0: decrement stashed");
            stash();
        }
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
