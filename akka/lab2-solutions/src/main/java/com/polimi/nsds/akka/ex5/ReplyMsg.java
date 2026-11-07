package com.polimi.nsds.akka.ex5;

import java.io.Serializable;

/**
 * Immutable reply message sent by {@link ServerActor} in response to a {@link GetMsg}
 * (Exercise 5, extending Exercise 3).
 * <p>
 * Indicates whether the requested contact was found ({@link #isFound()}) and carries either the
 * associated email address or a descriptive not-found status message.
 * </p>
 */
public class ReplyMsg implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Requested contact name
    private final String name;
    // Associated email address (or null if not found)
    private final String email;
    // True if the contact was found in the contact list, false otherwise
    private final boolean found;
    // Human-readable status or error message
    private final String statusMessage;

    /**
     * Private constructor used by the static factory methods {@link #success(String, String)}
     * and {@link #notFound(String)}.
     *
     * @param name          the requested contact name
     * @param email         the email address if found, or {@code null}
     * @param found         {@code true} if the contact exists, {@code false} otherwise
     * @param statusMessage status description
     */
    private ReplyMsg(String name, String email, boolean found, String statusMessage) {
        this.name = name;
        this.email = email;
        this.found = found;
        this.statusMessage = statusMessage;
    }

    /**
     * Builds a successful reply containing the contact's email address.
     *
     * @param name  the contact name
     * @param email the contact email address
     * @return a {@link ReplyMsg} with {@code found == true}
     */
    public static ReplyMsg success(String name, String email) {
        return new ReplyMsg(name, email, true, "OK");
    }

    /**
     * Builds a not-found reply when the contact name is absent from the server's map
     * (for instance, because it was never inserted or because a {@code restart()} wiped the map).
     *
     * @param name the missing contact name
     * @return a {@link ReplyMsg} with {@code found == false}
     */
    public static ReplyMsg notFound(String name) {
        return new ReplyMsg(name, null, false, "Error: Contact '" + name + "' not found in contact list");
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }

    public boolean isFound() {
        return found;
    }

    public String getStatusMessage() {
        return statusMessage;
    }
}
