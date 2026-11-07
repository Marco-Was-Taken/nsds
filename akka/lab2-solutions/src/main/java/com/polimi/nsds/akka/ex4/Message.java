package com.polimi.nsds.akka.ex4;

import java.io.Serializable;

/**
 * Base abstract class for all protocol messages exchanged between the client and the server
 * in Exercise 4 (Sleep/Wakeup Echo Server).
 * <p>
 * Implementing {@link Serializable} follows Akka best practices so that all subclasses
 * ({@link TextMessage}, {@link SleepMessage}, {@link WakeupMessage}) can be safely transmitted
 * across actor mailboxes.
 * </p>
 */
public abstract class Message implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;
}
