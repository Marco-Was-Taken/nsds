package com.polimi.nsds.akka.ex6;

/**
 * Immutable text message exchanged between {@link ClientActor} and {@link ServerActor} in Exercise 6.
 */
public class TextMessage extends Message {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Immutable text payload
    private final String text;

    /**
     * Creates a new immutable text message.
     *
     * @param text the text payload
     */
    public TextMessage(String text) {
        // Store the text payload
        this.text = text;
    }

    /**
     * Returns the text payload carried by this message.
     *
     * @return the message text
     */
    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return "TextMessage{text='" + text + "'}";
    }
}
