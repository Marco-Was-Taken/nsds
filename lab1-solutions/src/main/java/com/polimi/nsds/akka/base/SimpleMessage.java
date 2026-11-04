package com.polimi.nsds.akka.base;

import java.io.Serializable;

/**
 * Message used in the introductory counter example of the slides
 * ({@code counter.tell(new SimpleMessage(), ActorRef.noSender())}).
 * <p>
 * It carries no data: its only meaning is "increment the counter".
 * It has no mutable fields, so it is trivially immutable, as required by the
 * mailbox semantics (messages must be immutable; the framework cannot enforce it).
 * </p>
 */
public class SimpleMessage implements Serializable {

    // Serialization version identifier
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new SimpleMessage.
     */
    public SimpleMessage() {
        // No fields to initialize
    }
}
