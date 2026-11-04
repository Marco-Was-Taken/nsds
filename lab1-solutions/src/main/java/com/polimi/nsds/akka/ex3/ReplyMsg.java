package com.polimi.nsds.akka.ex3;

import java.io.Serializable;

/**
 * Immutable response message sent by {@link ServerActor} back to the client actor
 * in response to a {@link GetMsg} query.
 * <p>
 * Contains the queried contact name, the found email address (or null if absent),
 * a boolean flag indicating whether the contact was found, and an informative status/error message.
 * </p>
 */
public class ReplyMsg implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Contact name queried by the client
    private final String name;
    // Email address found (null if contact not found)
    private final String email;
    // Boolean indicator of query success
    private final boolean found;
    // Informative message detailing the lookup outcome or error description
    private final String statusMessage;

    /**
     * Constructs a ReplyMsg representing a successful contact lookup.
     *
     * @param name  the queried contact name
     * @param email the email address found in the contact list
     * @return a successful ReplyMsg instance
     */
    public static ReplyMsg success(String name, String email) {
        // Return a positive ReplyMsg
        return new ReplyMsg(name, email, true, "Contact found: " + email);
    }

    /**
     * Constructs a ReplyMsg representing an unsuccessful contact lookup (contact not found).
     *
     * @param name the queried contact name that was not found
     * @return a not-found ReplyMsg instance
     */
    public static ReplyMsg notFound(String name) {
        // Return a negative ReplyMsg with descriptive error text
        return new ReplyMsg(name, null, false, "Error: Contact '" + name + "' not found in contact list");
    }

    /**
     * Internal constructor initializing all immutable fields.
     *
     * @param name          contact name
     * @param email         contact email
     * @param found         true if found, false otherwise
     * @param statusMessage descriptive status message
     */
    public ReplyMsg(String name, String email, boolean found, String statusMessage) {
        // Assign contact name
        this.name = name;
        // Assign email address
        this.email = email;
        // Assign found flag
        this.found = found;
        // Assign status message
        this.statusMessage = statusMessage;
    }

    /**
     * Retrieves the queried contact name.
     *
     * @return the name string
     */
    public String getName() {
        // Return name reference
        return this.name;
    }

    /**
     * Retrieves the email address.
     *
     * @return the email string, or null if not found
     */
    public String getEmail() {
        // Return email reference
        return this.email;
    }

    /**
     * Indicates whether the contact was found.
     *
     * @return true if the contact exists, false otherwise
     */
    public boolean isFound() {
        // Return found boolean
        return this.found;
    }

    /**
     * Retrieves the human-readable status or error message.
     *
     * @return the status message string
     */
    public String getStatusMessage() {
        // Return status string
        return this.statusMessage;
    }

    /**
     * Returns a string representation of this reply message.
     *
     * @return formatted string
     */
    @Override
    public String toString() {
        // Return formatted representation
        return "ReplyMsg{name='" + name + "', email='" + email + "', found=" + found + ", status='" + statusMessage + "'}";
    }
}
