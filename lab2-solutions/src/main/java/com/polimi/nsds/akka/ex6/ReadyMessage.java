package com.polimi.nsds.akka.ex6;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Immutable self-notification message scheduled by {@link ServerActor} via Akka's scheduler
 * after the 500 ms processing delay expires (Exercise 6).
 * <p>
 * Because other messages may arrive while the server is busy, this message carries both the
 * original client {@link ActorRef} ({@code sender}) and the processed text ({@code message}) so
 * that {@link ServerActor} can reply to the right client when transitioning out of busy mode.
 * </p>
 */
public class ReadyMessage implements Serializable {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;

    // Reference to the original client that requested the processing
    private final ActorRef sender;
    // The message payload to send back to the client when ready
    private final String message;

    /**
     * Creates a new ready notification message.
     *
     * @param sender  the {@link ActorRef} of the client that sent the request
     * @param message the processed message text
     */
    public ReadyMessage(ActorRef sender, String message) {
        // Store original requester reference
        this.sender = sender;
        // Store message text
        this.message = message;
    }

    /**
     * Returns the {@link ActorRef} of the client that initiated the request.
     *
     * @return the original sender {@link ActorRef}
     */
    public ActorRef getSender() {
        return sender;
    }

    /**
     * Returns the message text associated with the completed task.
     *
     * @return the message string
     */
    public String getMessage() {
        return message;
    }
}
