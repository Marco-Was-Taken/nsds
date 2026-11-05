package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.IOException;
import java.io.Serializable;

/**
 * EXAM TEMPLATE 2: Dynamic Behavior / Finite State Machine (Lecture 1 - getContext().become())
 * <p>
 * Pattern:
 * - Define one {@code private Receive stateName()} method per state.
 * - Return the initial state from {@link #createReceive()}.
 * - Switch state dynamically via {@code getContext().become(nextState())}.
 * - Any message not matched in the current {@code Receive} behavior is automatically discarded.
 * </p>
 */
public class StateMachineActorTemplate extends AbstractActor {

    // --- Immutable Messages ---
    public static class ActivateMsg implements Serializable {}
    public static class DeactivateMsg implements Serializable {}
    public static class TriggerMsg implements Serializable {
        private final String info;
        public TriggerMsg(String info) { this.info = info; }
        public String getInfo() { return info; }
    }

    // --- Initial Behavior ---
    @Override
    public Receive createReceive() {
        return disabled();
    }

    // --- Behavior 1: DISABLED ---
    private final Receive disabled() {
        return receiveBuilder()
                .match(ActivateMsg.class, this::onActivate)
                // TriggerMsg is NOT matched here -> automatically discarded while disabled!
                .build();
    }

    // --- Behavior 2: ENABLED ---
    private final Receive enabled() {
        return receiveBuilder()
                .match(TriggerMsg.class, this::onTrigger)
                .match(DeactivateMsg.class, this::onDeactivate)
                .build();
    }

    // --- Handlers ---
    private void onActivate(ActivateMsg msg) {
        System.out.println("[StateMachine] Becoming ENABLED");
        getContext().become(enabled());
    }

    private void onDeactivate(DeactivateMsg msg) {
        System.out.println("[StateMachine] Becoming DISABLED");
        getContext().become(disabled());
    }

    private void onTrigger(TriggerMsg msg) {
        System.out.println("[StateMachine] ALARM TRIGGERED: " + msg.getInfo());
    }

    public static Props props() {
        return Props.create(StateMachineActorTemplate.class);
    }

    // --- Runnable Demo ---
    public static void main(String[] args) throws IOException {
        final ActorSystem sys = ActorSystem.create("StateMachineSystem");
        final ActorRef alarm = sys.actorOf(StateMachineActorTemplate.props(), "alarm");

        alarm.tell(new TriggerMsg("Ignored because disabled"), ActorRef.noSender());
        alarm.tell(new ActivateMsg(), ActorRef.noSender());
        alarm.tell(new TriggerMsg("Intruder detected!"), ActorRef.noSender());
        alarm.tell(new DeactivateMsg(), ActorRef.noSender());
        alarm.tell(new TriggerMsg("Ignored again"), ActorRef.noSender());

        System.out.println("Press ENTER to terminate...");
        System.in.read();
        sys.terminate();
    }
}
