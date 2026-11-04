package com.polimi.nsds.akka.ex1.selectors;

import java.io.Serializable;

/**
 * Single message class for Exercise 1, solution B (message selectors).
 * <p>
 * The operation is not encoded by the class but by the <b>content</b> of the message
 * (the {@code op} field). The actor distinguishes increments from decrements with
 * predicates on this field. The field is final, so the message is immutable.
 * </p>
 */
public class CounterMsg implements Serializable {

    // Serialization version identifier
    private static final long serialVersionUID = 1L;

    /**
     * Operations supported by the counter.
     */
    public enum Op {
        // Increment the counter by one
        INCREMENT,
        // Decrement the counter by one
        DECREMENT
    }

    // Requested operation, final to keep the message immutable
    private final Op op;

    /**
     * Creates a message carrying the requested operation.
     *
     * @param op the operation to perform
     */
    public CounterMsg(Op op) {
        // Store the operation
        this.op = op;
    }

    /**
     * Returns the requested operation; used by the predicates in the actor.
     *
     * @return the operation
     */
    public Op getOp() {
        // Return the immutable field
        return op;
    }
}
