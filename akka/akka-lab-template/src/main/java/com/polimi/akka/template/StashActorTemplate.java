package com.polimi.akka.template;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.IOException;
import java.io.Serializable;

/**
 * EXAM TEMPLATE 3: Stashing Messages (Lecture 2 - AbstractActorWithStash)
 * <p>
 * Pattern:
 * - Extend {@link AbstractActorWithStash} instead of {@code AbstractActor}.
 * - Call {@code stash()} when a message cannot be processed in the current state/behavior.
 * - Call {@code unstashAll()} when the state changes (e.g. after an increment, config init, or reply)
 *   to prepend all stashed messages back to the front of the mailbox in FIFO order.
 * </p>
 */
public class StashActorTemplate extends AbstractActorWithStash {

    // --- Immutable Messages ---
    public static class ProduceMsg implements Serializable {
        private final int amount;
        public ProduceMsg(int amount) { this.amount = amount; }
        public int getAmount() { return amount; }
    }

    public static class ConsumeMsg implements Serializable {
        private final int amount;
        public ConsumeMsg(int amount) { this.amount = amount; }
        public int getAmount() { return amount; }
    }

    // --- Private State ---
    private int balance = 0;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // Option A: Guard condition inside handler
                .match(ConsumeMsg.class, this::onConsume)
                .match(ProduceMsg.class, this::onProduce)
                .build();
    }

    private void onConsume(ConsumeMsg msg) {
        if (balance >= msg.getAmount()) {
            balance -= msg.getAmount();
            System.out.println("[StashActor] Consumed " + msg.getAmount() + " | balance = " + balance);
        } else {
            System.out.println("[StashActor] Insufficient balance (" + balance + " < " + msg.getAmount() + ") -> stash()");
            stash();
        }
    }

    private void onProduce(ProduceMsg msg) {
        balance += msg.getAmount();
        System.out.println("[StashActor] Produced " + msg.getAmount() + " | balance = " + balance + " -> unstashAll()");
        // Prepend all previously stashed ConsumeMsg back to the mailbox in FIFO order
        unstashAll();
    }

    public static Props props() {
        return Props.create(StashActorTemplate.class);
    }

    // --- Runnable Demo ---
    public static void main(String[] args) throws IOException {
        final ActorSystem sys = ActorSystem.create("StashSystem");
        final ActorRef actor = sys.actorOf(StashActorTemplate.props(), "stashActor");

        actor.tell(new ConsumeMsg(1), ActorRef.noSender()); // Stashed
        actor.tell(new ConsumeMsg(1), ActorRef.noSender()); // Stashed
        actor.tell(new ProduceMsg(1), ActorRef.noSender()); // 0 -> 1, unstashes first ConsumeMsg (1 -> 0), second re-stashed
        actor.tell(new ProduceMsg(1), ActorRef.noSender()); // 0 -> 1, unstashes second ConsumeMsg (1 -> 0)

        System.out.println("Press ENTER to terminate...");
        System.in.read();
        sys.terminate();
    }
}
