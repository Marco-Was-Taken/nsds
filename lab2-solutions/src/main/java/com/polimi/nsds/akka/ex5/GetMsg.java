package com.polimi.nsds.akka.ex5;

import java.io.Serializable;

/**
 * Immutable message requesting the email address associated with a given contact name
 * (Exercise 5, extending Exercise 3).
 * <p>
 * Upon receiving this message, {@link ServerActor} looks up {@code name} in its contact list
 * and replies to {@code sender()} with a {@link ReplyMsg}.
 * </p>
 */
public class GetMsg implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Contact name to look up, declared final to enforce immutability
    private final String name;

    /**
     * Constructs an immutable GetMsg instance.
     *
     * @param name the name of the contact to look up
     */
    public GetMsg(String name) {
        // Assign the contact name
        this.name = name;
    }

    /**
     * Retrieves the contact name to look up.
     *
     * @return the name string
     */
    public String getName() {
        // Return name reference
        return this.name;
    }

    /**
     * Returns a string representation of this lookup request.
     *
     * @return formatted string containing the contact name
     */
    @Override
    public String toString() {
        // Return formatted representation
        return "GetMsg{name='" + name + "'}";
    }
}
