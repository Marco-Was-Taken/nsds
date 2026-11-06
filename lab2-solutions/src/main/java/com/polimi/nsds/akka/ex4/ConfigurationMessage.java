package com.polimi.nsds.akka.ex4;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Immutable configuration message sent from {@code main} to {@link ClientActor} at startup (Exercise 4).
 * <p>
 * It provides the target {@link ServerActor} reference and the ordered sequence of {@link Message}s
 * ({@link TextMessage}, {@link SleepMessage}, {@link WakeupMessage}) that the client should send
 * to the server.
 * </p>
 */
public class ConfigurationMessage implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Reference to the target ServerActor
    private final ActorRef server;

    // Unmodifiable list of messages to be sent in sequence to the server
    private final List<Message> messages;

    /**
     * Creates a new configuration message with the server reference and message batch.
     *
     * @param server   the {@link ActorRef} of the server actor
     * @param messages the ordered list of messages to send
     */
    public ConfigurationMessage(ActorRef server, List<Message> messages) {
        // Store the target server reference
        this.server = server;
        // Wrap in an unmodifiable list to preserve message immutability
        this.messages = Collections.unmodifiableList(messages);
    }

    /**
     * Returns the target server actor reference.
     *
     * @return the server {@link ActorRef}
     */
    public ActorRef getServer() {
        return server;
    }

    /**
     * Returns the ordered sequence of messages to send to the server.
     *
     * @return unmodifiable list of {@link Message}s
     */
    public List<Message> getMessages() {
        return messages;
    }
}
