package com.polimi.nsds.akka.ex1.classes;

import java.io.Serializable;

/**
 * Message asking the counter to increment (Exercise 1, class-based solution).
 * <p>
 * The operation is encoded by the <b>class</b> of the message itself, so the actor can
 * distinguish it from {@link DecrementMsg} with a plain {@code match(IncrementMsg.class, ...)}.
 * The class has no fields, hence it is immutable.
 * </p>
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
