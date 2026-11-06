package com.polimi.nsds.akka.ex6;

import java.io.Serializable;

/**
 * Immutable reply message wrapping an upper-case converted string (Exercise 6).
 */
public class UpperTextReply implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Upper-case converted string payload
    private final String upperString;

    /**
     * Creates an immutable reply containing the upper-case text.
     *
     * @param upperString the upper-case string
     */
    public UpperTextReply(String upperString) {
        // Store upper-case string
        this.upperString = upperString;
    }

    /**
     * Returns the upper-case string.
     *
     * @return the upper-case text
     */
    public String upperString() {
        return upperString;
    }
}
