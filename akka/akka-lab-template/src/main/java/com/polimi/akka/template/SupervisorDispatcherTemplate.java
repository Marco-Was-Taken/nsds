package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * EXAM TEMPLATE 8: Supervisor Dispatcher with Round-Robin & Sticky Load Balancing (Evaluation Lab Pattern)
 * <p>
 * Combines Supervision, Dynamic Routing ({@code become()}), and Safe State Validation:
 * <ul>
 *   <li><b>Hierarchy of Responsibility:</b> Dispatcher spawns child workers inside {@link AbstractActor#preStart()}
 *       via {@code getContext().actorOf(...)} so it acts as their Supervisor.</li>
 *   <li><b>Supervision Strategy:</b> Uses {@link OneForOneStrategy} with {@code SupervisorStrategy.resume()}
 *       so a child keeps its accumulated state (e.g., running average) after discarding a faulty message.</li>
 *   <li><b>Validate Before Mutate:</b> The worker checks preconditions (e.g., {@code value < 0}) and throws
 *       <em>before</em> modifying its internal fields, ensuring {@code resume()} leaves state uncorrupted.</li>
 *   <li><b>Dynamic Policy Switching:</b> Switches between {@code loadBalancer()} (sticky per-sender routing)
 *       and {@code roundRobin()} via {@code getContext().become(...)}.</li>
 * </ul>
 * </p>
 */
public class SupervisorDispatcherTemplate {

    // --- Immutable Messages ---
    public static class SwitchPolicyMsg implements Serializable {
        public enum Policy { LOAD_BALANCER, ROUND_ROBIN }
        private final Policy policy;
        public SwitchPolicyMsg(Policy policy) { this.policy = policy; }
        public Policy getPolicy() { return policy; }
    }

    public static class ReadingMsg implements Serializable {
        private final String sensorId;
        private final double value;
        public ReadingMsg(String sensorId, double value) {
            this.sensorId = sensorId;
            this.value = value;
        }
        public String getSensorId() { return sensorId; }
        public double getValue() { return value; }
    }

    // =========================================================================
    // 1. DISPATCHER / SUPERVISOR ACTOR
    // =========================================================================
    public static class DispatcherActor extends AbstractActor {
        private static final int NUM_WORKERS = 2;

        private final List<ActorRef> workers = new ArrayList<>();
        // Sticky mapping: sensorId -> assigned worker ActorRef
        private final Map<String, ActorRef> stickyRoutes = new HashMap<>();
        private int rrIndex = 0;

        // Resume keeps the worker's running state while discarding the single faulty reading
        private static final SupervisorStrategy STRATEGY = new OneForOneStrategy(
                10,
                Duration.ofMinutes(1),
                DeciderBuilder
                        .match(Exception.class, e -> SupervisorStrategy.resume())
                        .build()
        );

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return STRATEGY;
        }

        @Override
        public void preStart() {
            for (int i = 0; i < NUM_WORKERS; i++) {
                workers.add(getContext().actorOf(WorkerActor.props(), "worker-" + i));
            }
        }

        @Override
        public Receive createReceive() {
            return loadBalancer(); // Default routing policy
        }

        private Receive loadBalancer() {
            return receiveBuilder()
                    .match(SwitchPolicyMsg.class,
                            m -> m.getPolicy() == SwitchPolicyMsg.Policy.ROUND_ROBIN,
                            m -> {
                                System.out.println("[Dispatcher] Switching to ROUND_ROBIN");
                                getContext().become(roundRobin());
                            })
                    .match(ReadingMsg.class, this::dispatchStickyLoadBalancer)
                    .build();
        }

        private Receive roundRobin() {
            return receiveBuilder()
                    .match(SwitchPolicyMsg.class,
                            m -> m.getPolicy() == SwitchPolicyMsg.Policy.LOAD_BALANCER,
                            m -> {
                                System.out.println("[Dispatcher] Switching to LOAD_BALANCER");
                                getContext().become(loadBalancer());
                            })
                    .match(ReadingMsg.class, this::dispatchRoundRobin)
                    .build();
        }

        private void dispatchStickyLoadBalancer(ReadingMsg msg) {
            final ActorRef assigned = stickyRoutes.computeIfAbsent(
                    msg.getSensorId(),
                    id -> workers.get(stickyRoutes.size() % workers.size())
            );
            assigned.tell(msg, sender());
        }

        private void dispatchRoundRobin(ReadingMsg msg) {
            final ActorRef worker = workers.get(rrIndex);
            rrIndex = (rrIndex + 1) % workers.size();
            worker.tell(msg, sender());
        }

        public static Props props() {
            return Props.create(DispatcherActor.class);
        }
    }

    // =========================================================================
    // 2. SUPERVISED WORKER ACTOR (Validates input BEFORE mutating state)
    // =========================================================================
    public static class WorkerActor extends AbstractActor {
        private double sum = 0.0;
        private int count = 0;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ReadingMsg.class, this::onReading)
                    .build();
        }

        private void onReading(ReadingMsg msg) throws Exception {
            // Check preconditions FIRST so resume() keeps previous sum/count uncorrupted!
            if (msg.getValue() < 0) {
                System.err.println("[" + self().path().name() + "] Faulty reading from "
                        + msg.getSensorId() + ": " + msg.getValue() + " -> throwing Exception");
                throw new Exception("Negative reading: " + msg.getValue());
            }

            sum += msg.getValue();
            count++;
            double avg = sum / count;
            System.out.println("[" + self().path().name() + "] Sensor=" + msg.getSensorId()
                    + " val=" + msg.getValue() + " | avg=" + avg + " (count=" + count + ")");
        }

        public static Props props() {
            return Props.create(WorkerActor.class);
        }
    }

    // --- Runnable Demo ---
    public static void main(String[] args) throws IOException {
        final ActorSystem sys = ActorSystem.create("DispatcherSystem");
        final ActorRef dispatcher = sys.actorOf(DispatcherActor.props(), "dispatcher");

        // 1. Sticky Load Balancing
        dispatcher.tell(new ReadingMsg("S1", 20.0), ActorRef.noSender());
        dispatcher.tell(new ReadingMsg("S1", 24.0), ActorRef.noSender());
        dispatcher.tell(new ReadingMsg("S1", -5.0), ActorRef.noSender()); // Resumed, avg stays 22.0!
        dispatcher.tell(new ReadingMsg("S1", 22.0), ActorRef.noSender()); // avg = (20+24+22)/3 = 22.0

        // 2. Switch to Round-Robin
        dispatcher.tell(new SwitchPolicyMsg(SwitchPolicyMsg.Policy.ROUND_ROBIN), ActorRef.noSender());
        dispatcher.tell(new ReadingMsg("S2", 10.0), ActorRef.noSender());
        dispatcher.tell(new ReadingMsg("S2", 30.0), ActorRef.noSender());

        System.out.println("Press ENTER to terminate...");
        System.in.read();
        sys.terminate();
    }
}
