package com.polimi.nsds.akka.ex4;

/**
 * Immutable control message instructing {@link ServerActor} to wake up from sleep mode (Exercise 4).
 * <p>
 * Upon receiving this message in sleep mode, {@link ServerActor} calls {@code unstashAll()} to
 * prepend all postponed {@link TextMessage}s back to the front of its mailbox in FIFO order and
 * reverts to its normal behavior via {@code getContext().become(createReceive())}.
 * </p>
 */
public class WakeupMessage extends Message {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;
}
