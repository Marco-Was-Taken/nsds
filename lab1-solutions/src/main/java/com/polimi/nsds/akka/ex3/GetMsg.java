package com.polimi.nsds.akka.ex3;

import java.io.Serializable;

/**
 * Immutable message requesting the email address of a contact identified by name.
 * <p>
 * This query message carries the lookup key. When received by the server actor,
 * it triggers a search in the server's internal storage and yields an asynchronous {@link ReplyMsg}.
 * </p>
 */
public class GetMsg implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Contact name to query, declared final for immutability
    private final String name;

    /**
     * Constructs an immutable GetMsg instance.
     *
     * @param name the contact name to search for
     */
    public GetMsg(String name) {
        // Assign the name safely
        this.name = name;
    }

    /**
     * Retrieves the contact name to search for.
     *
     * @return the name string
     */
    public String getName() {
        // Return name reference
        return this.name;
    }

    /**
     * Returns a string representation of this query message.
     *
     * @return formatted string containing the contact name
     */
    @Override
    public String toString() {
        // Return formatted representation
        return "GetMsg{name='" + name + "'}";
    }
}
