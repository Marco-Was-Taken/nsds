package com.polimi.nsds.akka.ex1.classes;

import akka.actor.AbstractActor;
import akka.actor.Props;

/**
 * Counter actor accepting increments and decrements as <b>different classes of messages</b>
 * (Exercise 1, solution A).
 * <p>
 * <b>How matching works (slide "Mailbox semantics"):</b> when a message arrives, the match
 * clauses are checked in the order in which they are defined, the handler of the first
 * matching clause is executed, and if no clause matches the message is discarded.
 * Here each clause simply checks the class of the message.
 * </p>
 * <p>
 * <b>Advantages of matching by class (vs. selectors that predicate on the content):</b>
 * <ul>
 *   <li>The intent of the message is visible in its type: the compiler checks that the
 *       handler receives the right type and refactoring tools can track every usage.</li>
 *   <li>Each message class carries only the data its operation needs (an increment and,
 *       for example, a reset could have completely different fields).</li>
 *   <li>An invalid operation simply cannot be built: there is no "unknown op code" value
 *       that could silently fall through all clauses and be discarded at runtime.</li>
 * </ul>
 * Typical use: a fixed set of operations known at design time (commands of a protocol,
 * request/reply pairs such as PutMsg/GetMsg/ReplyMsg in Exercise 3).
 * The cost is a larger number of (small) classes. Performance is not a real
 * discriminant: in both approaches the clauses are evaluated in order.
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
     * Defines two match clauses, one per message class.
     *
     * @return the Receive object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // Clauses are checked in order: IncrementMsg first, then DecrementMsg
        return receiveBuilder()
                .match(IncrementMsg.class, this::onIncrement)
                .match(DecrementMsg.class, this::onDecrement)
                .build();
    }

    /**
     * Handles an increment request atomically.
     *
     * @param msg the increment request
     */
    void onIncrement(IncrementMsg msg) {
        // Increment the private state; no lock needed because messages are processed one at a time
        ++counter;
        // Print the new value
        System.out.println("Counter increased to: " + counter);
    }

    /**
     * Handles a decrement request atomically.
     *
     * @param msg the decrement request
     */
    void onDecrement(DecrementMsg msg) {
        // Decrement the private state; no lock needed because messages are processed one at a time
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
