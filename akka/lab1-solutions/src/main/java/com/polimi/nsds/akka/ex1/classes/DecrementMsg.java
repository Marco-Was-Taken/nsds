package com.polimi.nsds.akka.ex1.classes;

import java.io.Serializable;

/**
 * Message asking the counter to decrement (Exercise 1, class-based solution).
 * <p>
 * The operation is encoded by the <b>class</b> of the message itself, so the actor can
 * distinguish it from {@link IncrementMsg} with a plain {@code match(DecrementMsg.class, ...)}.
 * The class has no fields, hence it is immutable.
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
