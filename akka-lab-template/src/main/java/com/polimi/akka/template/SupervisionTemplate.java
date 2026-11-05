package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllForOneStrategy;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import scala.concurrent.Future;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * EXAM TEMPLATE 5: Fault Tolerance & Supervision (Lecture 3 - Slides 9–13)
 * <p>
 * Key Concepts:
 * <ul>
 *   <li>Every actor is supervised by its parent (the actor that called {@code getContext().actorOf(props)}).</li>
 *   <li>{@link OneForOneStrategy}: applies the directive only to the failed child.</li>
 *   <li>{@link AllForOneStrategy}: applies the directive to the failed child AND all its siblings.</li>
 *   <li>Directives:
 *     <ul>
 *       <li>{@code SupervisorStrategy.resume()}: keeps the same child instance and internal state.</li>
 *       <li>{@code SupervisorStrategy.restart()}: replaces the child instance with a new one (resets state to initial values),
 *           preserving the same {@code ActorRef} and mailbox.</li>
 *       <li>{@code SupervisorStrategy.stop()}: permanently terminates the child actor.</li>
 *       <li>{@code SupervisorStrategy.escalate()}: propagates the failure to the supervisor's own parent.</li>
 *     </ul>
 *   </li>
 * </ul>
 * </p>
 */
public class SupervisionTemplate {

    public static final int NORMAL_OP = 0;
    public static final int FAULT_RESTART_OP = -1;
    public static final int FAULT_RESUME_OP = -2;
    public static final int FAULT_STOP_OP = -3;
    public static final int FAULTS = 1;

    // --- Immutable Message ---
    public static class DataMessage implements Serializable {
        private final int code;
        public DataMessage(int code) { this.code = code; }
        public int getCode() { return code; }
    }

    // =========================================================================
    // 1. SUPERVISOR ACTOR (Lecture 3, Slide 11)
    // =========================================================================
    public static class CounterSupervisorActor extends AbstractActor {

        // OneForOneStrategy: max 10 faults within 1 minute
        private static final SupervisorStrategy strategy =
                new OneForOneStrategy(
                        10,
                        Duration.ofMinutes(1),
                        DeciderBuilder
                                // Resume on ArithmeticException (state is preserved!)
                                .match(ArithmeticException.class, e -> SupervisorStrategy.resume())
                                // Stop on IllegalArgumentException (child terminated permanently)
                                .match(IllegalArgumentException.class, e -> SupervisorStrategy.stop())
                                // Restart on generic Exception (state is reset to 0!)
                                .match(Exception.class, e -> SupervisorStrategy.restart())
                                // Escalate anything else
                                .matchAny(o -> SupervisorStrategy.escalate())
                                .build()
                );

        // Alternative: AllForOneStrategy (applies decision to all children of this supervisor)
        @SuppressWarnings("unused")
        private static final SupervisorStrategy allForOneStrategy =
                new AllForOneStrategy(
                        10,
                        Duration.ofMinutes(1),
                        DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.restart()).build()
                );

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public Receive createReceive() {
            // Spawns a child actor from the received Props and returns the child's ActorRef to the requester
            return receiveBuilder()
                    .match(Props.class, props -> {
                        ActorRef child = getContext().actorOf(props);
                        sender().tell(child, self());
                    })
                    .build();
        }

        public static Props props() {
            return Props.create(CounterSupervisorActor.class);
        }
    }

    // =========================================================================
    // 2. SUPERVISED CHILD ACTOR (Lecture 3, Slide 12)
    // =========================================================================
    public static class CounterActor extends AbstractActor {
        private int counter = 0;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(DataMessage.class, this::onMessage)
                    .build();
        }

        void onMessage(DataMessage msg) throws Exception {
            if (msg.getCode() == NORMAL_OP) {
                System.out.println("I am executing a NORMAL operation...counter is now " + (++counter));
            } else if (msg.getCode() == FAULT_RESTART_OP) {
                System.out.println("I am emulating a FAULT (Restart)!");
                throw new Exception("Actor fault (triggers restart)!");
            } else if (msg.getCode() == FAULT_RESUME_OP) {
                System.out.println("I am emulating a minor FAULT (Resume)!");
                throw new ArithmeticException("Minor fault (triggers resume)!");
            } else if (msg.getCode() == FAULT_STOP_OP) {
                System.out.println("I am emulating a fatal FAULT (Stop)!");
                throw new IllegalArgumentException("Fatal fault (triggers stop)!");
            }
        }

        @Override
        public void preRestart(Throwable reason, Optional<Object> message) {
            System.out.print("Preparing to restart...");
        }

        @Override
        public void postRestart(Throwable reason) {
            System.out.println("...now restarted!");
        }

        public static Props props() {
            return Props.create(CounterActor.class);
        }
    }

    // =========================================================================
    // 3. MAIN BOOTSTRAPPER (Lecture 3, Slide 13)
    // =========================================================================
    public static void main(String[] args) {
        scala.concurrent.duration.Duration timeout =
                scala.concurrent.duration.Duration.create(5, SECONDS);

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef supervisor = sys.actorOf(CounterSupervisorActor.props(), "supervisor");

        ActorRef counter;
        try {
            // Ask supervisor to create the child inside the supervisor's context
            Future<Object> waitingForCounter = ask(supervisor, Props.create(CounterActor.class), 5000);
            counter = (ActorRef) waitingForCounter.result(timeout, null);

            // 1. Normal increment -> counter = 1
            counter.tell(new DataMessage(NORMAL_OP), ActorRef.noSender());

            // 2. Fault triggering resume -> counter stays 1
            counter.tell(new DataMessage(FAULT_RESUME_OP), ActorRef.noSender());
            counter.tell(new DataMessage(NORMAL_OP), ActorRef.noSender()); // counter = 2

            // 3. Fault triggering restart -> counter resets to 0
            for (int i = 0; i < FAULTS; i++) {
                counter.tell(new DataMessage(FAULT_RESTART_OP), ActorRef.noSender());
            }

            // 4. Normal increment after restart -> counter = 1
            counter.tell(new DataMessage(NORMAL_OP), ActorRef.noSender());

            Thread.sleep(1000);
            sys.terminate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
