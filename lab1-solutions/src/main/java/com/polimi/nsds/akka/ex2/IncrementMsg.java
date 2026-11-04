package com.polimi.nsds.akka.ex2;

import java.io.Serializable;

/**
 * Increment request for Exercise 2. Immutable (no fields).
 */
public class IncrementMsg implements Serializable {

    // Serialization version identifier
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new increment request.
     */
    public IncrementMsg() {
        // No fields to initialize
    }
}
