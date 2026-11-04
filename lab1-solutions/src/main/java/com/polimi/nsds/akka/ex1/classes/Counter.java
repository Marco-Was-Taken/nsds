package com.polimi.nsds.akka.ex1.classes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test for Exercise 1, solution A (different message classes).
 * <p>
 * As suggested in the lab slides, the {@code exec.submit()} line of the original example is
 * duplicated inside the for loop: one task sends an {@link IncrementMsg}, the other a
 * {@link DecrementMsg}. The interleaving of increments and decrements is non-deterministic
 * (messages come from different senders), so intermediate values vary between runs, but
 * every message is processed atomically and the final value is always 0.
 * </p>
 */
public class Counter {

    // Number of threads sending messages concurrently
    private static final int numThreads = 16;
    // Number of loop iterations (each iteration sends one increment and one decrement)
    private static final int numMessages = 1000;

    /**
     * Creates the actor system and the counter, sends increments and decrements in parallel,
     * waits for ENTER and terminates.
     *
     * @param args command-line arguments (not used)
     * @throws InterruptedException never thrown here, kept for consistency with the slides
     * @throws IOException          if reading from standard input fails
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        // Create the actor system and the counter actor
        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef counter = sys.actorOf(CounterActor.props(), "counter");

        // Send messages from multiple threads in parallel
        final ExecutorService exec = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numMessages; i++) {
            exec.submit(() -> counter.tell(new IncrementMsg(), ActorRef.noSender()));
            exec.submit(() -> counter.tell(new DecrementMsg(), ActorRef.noSender()));
        }

        // Wait for all messages to be sent and received
        System.out.println("Press ENTER to terminate...");
        System.in.read();

        // Stop the thread pool and the actor system
        exec.shutdown();
        sys.terminate();
    }
}
