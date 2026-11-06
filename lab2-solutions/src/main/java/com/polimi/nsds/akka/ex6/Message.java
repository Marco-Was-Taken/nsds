package com.polimi.nsds.akka.ex6;

import java.io.Serializable;

/**
 * Base abstract class for messages sent from clients to the server in Exercise 6.
 * <p>
 * Implements {@link Serializable} in accordance with Akka best practices.
 * </p>
 */
public abstract class Message implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;
}
