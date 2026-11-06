package com.polimi.nsds.akka.ex4;

/**
 * Immutable control message instructing {@link ServerActor} to transition from its normal (awake)
 * behavior to its sleeping behavior via {@code getContext().become(onSleepMode())} (Exercise 4).
 * <p>
 * Any {@link TextMessage} received after this message will be stashed until a {@link WakeupMessage}
 * is received.
 * </p>
 */
public class SleepMessage extends Message {

    // Serial version UID for Java serialization
    private static final long serialVersionUID = 1L;
}
