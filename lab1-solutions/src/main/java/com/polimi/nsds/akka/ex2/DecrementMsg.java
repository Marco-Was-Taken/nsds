package com.polimi.nsds.akka.ex2;

import java.io.Serializable;

/**
 * Decrement request for Exercise 2. Immutable (no fields).
 * <p>
 * If it arrives when the counter is 0 it is stashed and processed later.
 * </p>
 */
public class DecrementMsg implements Serializable {

    // Serialization version identifier
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new decrement request.
     */
    public DecrementMsg() {
        // No fields to initialize
    }
}
