package com.polimi.nsds.akka.ex1.selectors;

import akka.actor.AbstractActor;
import akka.actor.Props;

/**
 * Counter actor using <b>message selectors that predicate on the content of messages</b>
 * (Exercise 1, solution B).
 * <p>
 * Both clauses match the same class ({@link CounterMsg}); the overload
 * {@code match(Class, Predicate, handler)} adds a predicate that is evaluated on the message
 * after the class check. As for any clause, they are checked in definition order, the first
 * clause whose class and predicate both match wins, and a message matching no clause
 * (e.g. a future op value not handled here) is discarded.
 * </p>
 * <p>
 * <b>When selectors are preferable to separate classes:</b>
 * <ul>
 *   <li>The distinguishing information is naturally data, e.g. an op code coming from a
 *       wire protocol, or a value-based condition (amount &gt; threshold, priority, ...).</li>
 *   <li>Many variants share exactly the same fields, so one class avoids duplication.</li>
 *   <li>The same message type must be routed to different handlers depending on its value.</li>
 * </ul>
 * <b>Drawbacks:</b> correctness is checked only at runtime (an unhandled value is silently
 * discarded), the predicates must be kept mutually exclusive or carefully ordered, and one
 * class must contain the fields of every variant. Performance differences are negligible:
 * a predicate is just one extra check after the class match.
 * </p>
 */
public class CounterActor extends AbstractActor {

    // Private state of the actor
    private int counter;

    /**
     * Creates the actor and initializes its state.
     */
    public CounterActor() {
        // Initial value of the counter
        this.counter = 0;
    }

    /**
     * Defines two clauses on the same class, filtered by predicates on the message content.
     *
     * @return the Receive object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // Same class, different predicates on the op field
        return receiveBuilder()
                .match(CounterMsg.class, msg -> msg.getOp() == CounterMsg.Op.INCREMENT, this::onIncrement)
                .match(CounterMsg.class, msg -> msg.getOp() == CounterMsg.Op.DECREMENT, this::onDecrement)
                .build();
    }

    /**
     * Handles an increment request atomically.
     *
     * @param msg message whose op is INCREMENT
     */
    void onIncrement(CounterMsg msg) {
        // Increment the private state; no lock needed
        ++counter;
        // Print the new value
        System.out.println("Counter increased to: " + counter);
    }

    /**
     * Handles a decrement request atomically.
     *
     * @param msg message whose op is DECREMENT
     */
    void onDecrement(CounterMsg msg) {
        // Decrement the private state; no lock needed
        --counter;
        // Print the new value
        System.out.println("Counter decreased to: " + counter);
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
