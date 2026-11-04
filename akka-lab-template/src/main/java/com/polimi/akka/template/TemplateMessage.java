package com.polimi.akka.template;

import java.io.Serializable;

/**
 * Immutable and serializable message class representing a generic data payload
 * exchanged between Akka actors.
 * <p>
 * In the Akka Actor Model, messages MUST be immutable to prevent shared-memory
 * concurrency issues between sender and receiver. Because messages cannot be modified
 * once created, multiple actors or threads can safely read message contents without
 * any need for synchronization or defensive copying.
 * </p>
 */
public class TemplateMessage implements Serializable {

    // Version UID to guarantee serialization compatibility across JVM instances
    private static final long serialVersionUID = 1L;

    // The textual payload of the message, declared final to enforce immutability
    private final String content;

    /**
     * Constructs a new immutable TemplateMessage instance.
     *
     * @param content the string payload carried by this message
     */
    public TemplateMessage(String content) {
        // Assign the payload to the final field ensuring safe publication across threads
        this.content = content;
    }

    /**
     * Retrieves the immutable content payload of this message.
     *
     * @return the string content payload
     */
    public String getContent() {
        // Return the payload reference directly because String is immutable in Java
        return this.content;
    }

    /**
     * Returns a string representation of this message for logging and debugging purposes.
     *
     * @return a formatted string displaying the message payload
     */
    @Override
    public String toString() {
        // Return human-readable representation of this message instance
        return "TemplateMessage{content='" + content + "'}";
    }
}
