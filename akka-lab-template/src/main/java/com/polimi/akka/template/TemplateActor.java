package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.Props;

/**
 * Basic template actor following the structure presented in the course slides
 * ("Create actors" and "Actors basics").
 * <p>
 * An actor is defined by inheriting from {@link AbstractActor} and implementing
 * {@link #createReceive()}, which declares how each message type is processed.
 * Messages are processed atomically: the whole handler runs as if it were a
 * {@code synchronized} method of a class whose methods are all synchronized, and
 * messages received in the meantime are queued in the mailbox without affecting
 * the handler that is currently running.
 * </p>
 * <p>
 * The lifecycle hooks {@link #preStart()} and {@link #postStop()} are overridden to
 * show where custom logic can be placed when the actor enters different lifecycle states.
 * </p>
 */
public class TemplateActor extends AbstractActor {

    /**
     * Creates a new TemplateActor. Actors are never instantiated with {@code new}
     * by user code: Akka calls this constructor when {@code actorOf(props)} is invoked.
     */
    public TemplateActor() {
        // No state to initialize in the template actor
    }

    /**
     * Defines the receive behavior of the actor.
     * <p>
     * When a message arrives, the match clauses are checked in the order in which they
     * are defined and the handler of the first matching clause is executed.
     * If no clause matches, the message is discarded.
     * </p>
     *
     * @return the Receive object containing the match clauses
     */
    @Override
    public Receive createReceive() {
        // Build the behavior with a single clause matching TemplateMessage
        return receiveBuilder()
                .match(TemplateMessage.class, this::onMessage)
                .build();
    }

    /**
     * Handles a {@link TemplateMessage}. The method runs atomically with respect to
     * any other message delivered to this actor.
     *
     * @param msg the immutable message received from the mailbox
     */
    void onMessage(TemplateMessage msg) {
        // Print the content of the received message
        System.out.println("[TemplateActor] Received: " + msg.getContent());
    }

    /**
     * Lifecycle hook invoked by Akka after the actor instance is created and before
     * it processes its first message.
     */
    @Override
    public void preStart() {
        // Notify that the actor has started
        System.out.println("[TemplateActor] preStart(): actor started at " + self().path());
    }

    /**
     * Lifecycle hook invoked by Akka after the actor has been stopped
     * (for example when the ActorSystem terminates).
     */
    @Override
    public void postStop() {
        // Notify that the actor has stopped
        System.out.println("[TemplateActor] postStop(): actor stopped");
    }

    /**
     * Factory method returning the Props (the "property object" that defines the class
     * of the actor) used by {@code actorOf()} to create a new TemplateActor.
     *
     * @return Props for TemplateActor
     */
    static Props props() {
        // Same form used in the slides: Props.create(ActorClass.class)
        return Props.create(TemplateActor.class);
    }
}
