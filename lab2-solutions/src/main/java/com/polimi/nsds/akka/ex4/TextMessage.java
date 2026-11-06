package com.polimi.nsds.akka.ex4;

/**
 * Immutable message carrying a text payload exchanged between {@link ClientActor} and {@link ServerActor}
 * (Exercise 4).
 * <p>
 * When {@link ServerActor} is in its normal (awake) state, it immediately echoes this message back
 * to the sender; when {@link ServerActor} is in sleep mode, incoming {@code TextMessage}s are stashed
 * until a {@link WakeupMessage} arrives.
 * </p>
 */
public class TextMessage extends Message {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Immutable text payload
    private final String text;

    /**
     * Creates a new immutable text message.
     *
     * @param text the text content of the message
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
