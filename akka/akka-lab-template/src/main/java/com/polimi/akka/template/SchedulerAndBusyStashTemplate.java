package com.polimi.akka.template;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

/**
 * EXAM TEMPLATE 7: Non-Blocking Scheduler, Busy-Mode & Sleep/Wakeup Stashing (Lab 2 Ex 4 & Ex 6)
 * <p>
 * Combines two fundamental Lab 2 patterns using {@link AbstractActorWithStash}, {@code become()}, and {@code scheduler()}:
 * <ol>
 *   <li><b>Non-Blocking Delayed Processing (Lab 2 Ex 6):</b>
 *       Never call {@code Thread.sleep()} inside an actor (it blocks the dispatcher thread and freezes the mailbox).
 *       Instead, switch to {@code busy()} via {@code getContext().become(busy())}, schedule a {@link ReadyMsg} to
 *       {@code self()} with {@code getContext().getSystem().scheduler().scheduleOnce(...)}, and save the original
 *       {@code sender()} inside {@link ReadyMsg}. Any concurrent requests arriving while busy are postponed via {@code stash()}.</li>
 *   <li><b>Sleep / Wakeup Buffering (Lab 2 Ex 4):</b>
 *       Upon receiving {@link SleepMsg}, switch to {@code sleeping()}, {@code stash()} incoming work requests, and on
 *       {@link WakeupMsg} call {@code unstashAll()} followed by {@code getContext().become(idle())}.</li>
 * </ol>
 * </p>
 */
public class SchedulerAndBusyStashTemplate {

    // --- Immutable Messages ---
    public static class WorkMsg implements Serializable {
        private final String text;
        public WorkMsg(String text) { this.text = text; }
        public String getText() { return text; }
    }

    public static class ReplyMsg implements Serializable {
        private final String result;
        public ReplyMsg(String result) { this.result = result; }
        public String getResult() { return result; }
    }

    public static class SleepMsg implements Serializable {}
    public static class WakeupMsg implements Serializable {}

    // Internal timer message carrying the original sender() and computed result
    public static class ReadyMsg implements Serializable {
        private final ActorRef originalSender;
        private final String result;
        public ReadyMsg(ActorRef originalSender, String result) {
            this.originalSender = originalSender;
            this.result = result;
        }
        public ActorRef getOriginalSender() { return originalSender; }
        public String getResult() { return result; }
    }

    // =========================================================================
    // SERVER ACTOR (Idle <-> Busy with Scheduler, Idle <-> Sleeping with Stash)
    // =========================================================================
    public static class DelayedServerActor extends AbstractActorWithStash {

        private static final long PROCESSING_DELAY_MS = 500;

        @Override
        public Receive createReceive() {
            return idle();
        }

        private Receive idle() {
            return receiveBuilder()
                    .match(WorkMsg.class, this::onWork)
                    .match(SleepMsg.class, m -> {
                        System.out.println("[DelayedServer] Entering SLEEP mode...");
                        getContext().become(sleeping());
                    })
                    .build();
        }

        private void onWork(WorkMsg msg) {
            final String processed = msg.getText().toUpperCase();
            final ActorRef client = sender(); // Capture original sender BEFORE switching behavior

            System.out.println("[DelayedServer] Started async work on '" + msg.getText() + "' -> becoming BUSY");
            getContext().become(busy());

            // Schedule ReadyMsg to self() after 500ms WITHOUT blocking the actor thread
            getContext().getSystem().scheduler().scheduleOnce(
                    Duration.ofMillis(PROCESSING_DELAY_MS),
                    self(),
                    new ReadyMsg(client, processed),
                    getContext().dispatcher(),
                    self()
            );
        }

        private Receive busy() {
            return receiveBuilder()
                    // Stash concurrent WorkMsg or SleepMsg while processing the current request
                    .match(WorkMsg.class, m -> {
                        System.out.println("[DelayedServer] Busy -> stashing '" + m.getText() + "'");
                        stash();
                    })
                    .match(SleepMsg.class, m -> stash())
                    .match(ReadyMsg.class, this::onReady)
                    .build();
        }

        private void onReady(ReadyMsg ready) {
            // 1. Reply to the original client
            ready.getOriginalSender().tell(new ReplyMsg(ready.getResult()), self());
            // 2. Always unstashAll() BEFORE switching back to idle()
            unstashAll();
            getContext().become(idle());
        }

        private Receive sleeping() {
            return receiveBuilder()
                    .match(WorkMsg.class, m -> {
                        System.out.println("[DelayedServer] Sleeping -> stashing '" + m.getText() + "'");
                        stash();
                    })
                    .match(WakeupMsg.class, m -> {
                        System.out.println("[DelayedServer] WAKING UP -> unstashing all buffered requests");
                        unstashAll();
                        getContext().become(idle());
                    })
                    .build();
        }

        public static Props props() {
            return Props.create(DelayedServerActor.class);
        }
    }

    // =========================================================================
    // CLIENT ACTOR
    // =========================================================================
    public static class ClientActor extends AbstractActorWithStash {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ReplyMsg.class, r -> System.out.println("[Client] Received reply: " + r.getResult()))
                    .build();
        }

        public static Props props() {
            return Props.create(ClientActor.class);
        }
    }

    // --- Runnable Demo ---
    public static void main(String[] args) throws IOException {
        final ActorSystem sys = ActorSystem.create("SchedulerBusySystem");
        final ActorRef server = sys.actorOf(DelayedServerActor.props(), "delayedServer");
        final ActorRef client = sys.actorOf(ClientActor.props(), "client");

        // Burst of parallel messages: first runs immediately, others are stashed while busy
        server.tell(new WorkMsg("first"), client);
        server.tell(new WorkMsg("second"), client);
        server.tell(new SleepMsg(), ActorRef.noSender());
        server.tell(new WorkMsg("during-sleep"), client);

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        server.tell(new WakeupMsg(), ActorRef.noSender());

        System.out.println("Press ENTER to terminate...");
        System.in.read();
        sys.terminate();
    }
}
