package com.polimi.nsds.akka.ex5;

import java.io.Serializable;

/**
 * Immutable message requesting the storage of a contact entry (name and email) in the contact list
 * (Exercise 5, extending Exercise 3).
 * <p>
 * In Exercise 5, when {@code name.equals("Fail!")}, {@link ServerActor} emulates a fault by throwing
 * a generic {@link Exception}, which triggers the supervision strategy defined in {@link SupervisorActor}.
 * </p>
 */
public class PutMsg implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Contact name (key), declared final to enforce immutability
    private final String name;
    // Contact email (value), declared final to enforce immutability
    private final String email;

    /**
     * Constructs an immutable PutMsg instance.
     *
     * @param name  the name of the contact to store (or {@code "Fail!"} to trigger a fault)
     * @param email the email address associated with the contact
     */
    public PutMsg(String name, String email) {
        // Assign the contact name
        this.name = name;
        // Assign the contact email
        this.email = email;
    }

    /**
     * Retrieves the contact name.
     *
     * @return the name string
     */
    public String getName() {
        // Return name reference
        return this.name;
    }

    /**
     * Retrieves the contact email address.
     *
     * @return the email string
     */
    public String getEmail() {
        // Return email reference
        return this.email;
    }

    /**
     * Returns a string representation of this put message.
     *
     * @return formatted string containing name and email
     */
    @Override
    public String toString() {
        // Return formatted representation
        return "PutMsg{name='" + name + "', email='" + email + "'}";
    }
}
