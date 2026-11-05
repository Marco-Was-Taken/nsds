package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.Optional;

/**
 * EXAM TEMPLATE 1: Standard Actor (AbstractActor)
 * <p>
 * Demonstrates:
 * - Private mutable state (thread-safe without locks due to atomic message processing)
 * - Class-based matching and Predicate-based matching in {@link #createReceive()}
 * - Replying to the sender via {@code sender().tell(reply, self())}
 * - Lifecycle hooks: {@code preStart()}, {@code preRestart()}, {@code postRestart()}, {@code postStop()}
 * - Static {@code props()} factory methods (with and without constructor parameters)
 * </p>
 */
public class TemplateActor extends AbstractActor {

    private int counter = 0;
    private final String actorTag;

    // Default constructor
    public TemplateActor() {
        this.actorTag = "default";
    }

    // Parameterized constructor (use when passing initial config or ActorRefs at creation)
    public TemplateActor(String actorTag) {
        this.actorTag = actorTag;
    }

    @Override
    public Receive createReceive() {
        // Clauses are evaluated top-to-bottom; the FIRST matching clause is executed.
        // Unmatched messages are silently discarded unless .matchAny(...) is added.
        return receiveBuilder()
                // 1. Match with a boolean predicate (selector)
                .match(TemplateMessage.class, msg -> msg.getCode() < 0, this::onNegativeCodeMessage)
                // 2. Match by message class
                .match(TemplateMessage.class, this::onMessage)
                // 3. Optional catch-all for unexpected messages
                .matchAny(msg -> System.out.println("[" + actorTag + "] Ignored unknown message: " + msg))
                .build();
    }

    private void onMessage(TemplateMessage msg) {
        ++counter;
        System.out.println("[" + actorTag + "] Received: " + msg.getContent() + " | counter = " + counter);

        // If the sender is a real actor (not ActorRef.noSender()), send a reply
        if (!sender().equals(getContext().getSystem().deadLetters())) {
            sender().tell(new TemplateMessage("ACK #" + counter, counter), self());
        }
    }

    private void onNegativeCodeMessage(TemplateMessage msg) {
        System.out.println("[" + actorTag + "] Special handler for negative code: " + msg.getCode());
    }

    // --- Lifecycle Hooks ---

    @Override
    public void preStart() {
        System.out.println("[" + actorTag + "] preStart() at " + self().path());
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        // Called on the OLD instance before restart
        System.out.println("[" + actorTag + "] preRestart() due to: " + reason.getMessage());
    }

    @Override
    public void postRestart(Throwable reason) {
        // Called on the NEW instance after restart
        System.out.println("[" + actorTag + "] postRestart() completed");
    }

    @Override
    public void postStop() {
        System.out.println("[" + actorTag + "] postStop()");
    }

    // --- Props Factories ---

    public static Props props() {
        return Props.create(TemplateActor.class);
    }

    public static Props props(String actorTag) {
        return Props.create(TemplateActor.class, () -> new TemplateActor(actorTag));
    }
}

