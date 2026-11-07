package com.polimi.nsds.akka.ex6;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.List;

/**
 * Main entry point and test driver for Exercise 6 (Asynchronous Server Processing with Multiple Clients).
 *
 * <h2>Architectural Overview</h2>
 * <p>
 * The program creates a single {@link ServerActor} and two concurrent client actors
 * ({@code ClientActor1} and {@code ClientActor2}). Both clients are configured via
 * {@link ConfigurationMessage} to send two {@link TextMessage}s each ({@code "Hello1"}, {@code "Hi1"}
 * from {@code ClientActor1} and {@code "Hello2"}, {@code "Hi2"} from {@code ClientActor2}).
 * </p>
 *
 * <h2>Step-by-Step Execution Trace</h2>
 * <ol>
 *   <li><b>Concurrent Request Submission:</b>
 *       {@code ClientActor1} and {@code ClientActor2} concurrently send their {@link TextMessage}s to
 *       {@link ServerActor}. Messages from the same client arrive in FIFO order, while the interleaving
 *       between {@code ClientActor1} and {@code ClientActor2} is non-deterministic (Lecture 1, Slide 23).</li>
 *   <li><b>First Request &rarr; Busy Mode + Scheduler:</b>
 *       When {@link ServerActor} (in idle mode) dequeues the first {@link TextMessage}, it switches to
 *       {@code onBusyMode()} via {@code getContext().become(onBusyMode())} and schedules a
 *       {@link ReadyMessage} to {@code self()} 500 ms in the future.</li>
 *   <li><b>Stashing Concurrent Arrivals While Busy:</b>
 *       While the 500 ms timer is running, the remaining 3 {@link TextMessage}s arrive at
 *       {@link ServerActor}. Because the server is in {@code onBusyMode()}, each of those messages
 *       matches {@code onStashMessage} and is saved in the internal stash via {@code stash()}.</li>
 *   <li><b>Completion &amp; Sequential Drain via {@code unstashAll()}:</b>
 *       After 500 ms, {@link ServerActor} receives {@link ReadyMessage}, calls {@code unstashAll()}
 *       (returning the stashed {@link TextMessage}s to the front of its mailbox in FIFO order), switches
 *       back to idle mode ({@code createReceive()}), and replies to the first client. It then immediately
 *       dequeues the next unstashed {@link TextMessage}, enters {@code onBusyMode()} for another 500 ms
 *       (re-stashing the remaining requests), and repeats until all 4 requests have been processed
 *       one at a time at 500 ms intervals.</li>
 * </ol>
 */
public class ProcessingApp {

    /**
     * Bootstraps the ActorSystem with one server and two clients, dispatches concurrent batches,
     * waits for ENTER and terminates.
     *
     * @param args command-line arguments (not used)
     * @throws IOException if reading from standard input fails
     */
    public static void main(String[] args) throws IOException {
        // 1. Create the local ActorSystem container
        ActorSystem system = ActorSystem.create("ServerSleepWake");

        // 2. Instantiate one ServerActor and two concurrent ClientActors
        ActorRef server = system.actorOf(ServerActor.props(), "ServerActor");
        ActorRef client1 = system.actorOf(ClientActor.props(), "ClientActor1");
        ActorRef client2 = system.actorOf(ClientActor.props(), "ClientActor2");

        // 3. Configure ClientActor1 to send "Hello1" and "Hi1" to the server
        client1.tell(new ConfigurationMessage(
                server,
                List.of(
                        new TextMessage("Hello1"),
                        new TextMessage("Hi1")
                )
        ), ActorRef.noSender());

        // 4. Configure ClientActor2 to send "Hello2" and "Hi2" to the server concurrently
        client2.tell(new ConfigurationMessage(
                server,
                List.of(
                        new TextMessage("Hello2"),
                        new TextMessage("Hi2")
                )
        ), ActorRef.noSender());

        // 5. Wait for user input (at least ~2 seconds for 4 x 500 ms tasks) before terminating
        System.out.println("Press ENTER to terminate...");
        System.in.read();

        // 6. Stop all actors and release the ActorSystem dispatcher threads
        system.terminate();
    }
}
