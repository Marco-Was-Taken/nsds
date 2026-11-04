package com.polimi.nsds.akka.base;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Main class of the introductory example ("Actors basics" slide).
 * <p>
 * 16 threads send 1000 {@link SimpleMessage}s in parallel to the same {@link CounterActor}.
 * Even though the senders are concurrent, the actor processes the messages one at a time,
 * so the final value printed is always exactly 1000 (no lost updates).
 * </p>
 */
public class Counter {

    // Number of threads sending messages concurrently
    private static final int numThreads = 16;
    // Number of messages to send
    private static final int numMessages = 1000;

    /**
     * Creates the actor system and the counter, sends the messages in parallel,
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
            exec.submit(() -> counter.tell(new SimpleMessage(), ActorRef.noSender()));
        }

        // Wait for all messages to be sent and received
        System.out.println("Press ENTER to terminate...");
        System.in.read();

        // Stop the thread pool and the actor system
        exec.shutdown();
        sys.terminate();
    }
}
