package com.polimi.nsds.akka.ex4;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.List;

/**
 * Main entry point and test driver for Exercise 4 (Stateful Sleep/Wakeup Echo Server).
 *
 * <h2>Architectural Overview</h2>
 * <p>
 * The program creates a {@link ServerActor} (which extends {@code AbstractActorWithStash}) and a
 * {@link ClientActor} inside a local {@link ActorSystem}. The {@code main} thread sends a single
 * {@link ConfigurationMessage} to {@link ClientActor} containing the server's {@link ActorRef} and
 * an ordered list of 6 messages ({@link TextMessage}, {@link SleepMessage}, {@link WakeupMessage}).
 * </p>
 *
 * <h2>Step-by-Step Execution Trace</h2>
 * <ol>
 *   <li><b>Configuration &amp; FIFO Dispatch:</b>
 *       {@link ClientActor} receives the {@link ConfigurationMessage}, stores the {@code server}
 *       reference, and sends all 6 messages to {@link ServerActor} in a loop using
 *       {@code server.tell(textMessage, getSelf())}. Because all 6 messages come from the same sender
 *       ({@code ClientActor}), Akka's default FIFO mailbox policy (Lecture 1, Slide 23) guarantees
 *       they arrive in {@code ServerActor}'s mailbox in exact order:
 *       <ol>
 *         <li>{@code TextMessage("Hello")}</li>
 *         <li>{@code TextMessage("Hi")}</li>
 *         <li>{@code SleepMessage()}</li>
 *         <li>{@code TextMessage("Hello")}</li>
 *         <li>{@code TextMessage("Hello")}</li>
 *         <li>{@code WakeupMessage()}</li>
 *       </ol>
 *   </li>
 *   <li><b>Messages 1 &amp; 2 ({@code "Hello"}, {@code "Hi"}) in Awake Mode:</b>
 *       {@link ServerActor} starts in {@code createReceive()} (awake mode). It matches the first two
 *       {@link TextMessage}s via {@code onTextMessage} and immediately echoes them back to
 *       {@code sender()} ({@code ClientActor}), which prints {@code "Hello"} and {@code "Hi"}.</li>
 *   <li><b>Message 3 ({@code SleepMessage}):</b>
 *       {@link ServerActor} matches {@link SleepMessage} via {@code onSleepMessage} and calls
 *       {@code getContext().become(onSleepMode())}, switching its active behavior to sleep mode.</li>
 *   <li><b>Messages 4 &amp; 5 ({@code "Hello"}, {@code "Hello"}) in Sleep Mode:</b>
 *       While in {@code onSleepMode()}, {@link ServerActor} matches incoming {@link TextMessage}s
 *       with {@code onStashMessage}, which invokes {@code stash()} (Lecture 2, Slide 9). Neither
 *       message is echoed yet; both are stored in FIFO order in the actor's internal stash buffer.</li>
 *   <li><b>Message 6 ({@code WakeupMessage}):</b>
 *       Still in {@code onSleepMode()}, {@link ServerActor} matches {@link WakeupMessage} via
 *       {@code onWakeMessage}:
 *       <ul>
 *         <li>It calls {@code unstashAll()}, which extracts the two stashed {@code TextMessage("Hello")}
 *             instances in FIFO order and prepends them to the front of {@code ServerActor}'s mailbox;</li>
 *         <li>It calls {@code getContext().become(createReceive())} to return to awake mode;</li>
 *         <li>Now back in awake mode, {@link ServerActor} dequeues the two unstashed {@code TextMessage("Hello")}
 *             messages and echoes both back to {@link ClientActor}, which prints them.</li>
 *       </ul>
 *   </li>
 * </ol>
 */
public class ContactListApp {

    /**
     * Bootstraps the ActorSystem, runs the sleep/wakeup echo sequence, waits for ENTER and terminates.
     *
     * @param args command-line arguments (not used)
     * @throws IOException if reading from standard input fails
     */
    public static void main(String[] args) throws IOException {
        // 1. Create the local ActorSystem container
        ActorSystem system = ActorSystem.create("ServerSleepWake");

        // 2. Instantiate the ServerActor (with Stash) and the ClientActor
        ActorRef server = system.actorOf(ServerActor.props(), "ServerActor");
        ActorRef client = system.actorOf(ClientActor.props(), "ClientActor");

        // 3. Send the ConfigurationMessage to the client with the server reference and test sequence:
        //    - "Hello" and "Hi" are echoed immediately while the server is awake
        //    - SleepMessage puts the server into onSleepMode()
        //    - The next two "Hello" messages are stashed while the server is sleeping
        //    - WakeupMessage triggers unstashAll() + become(createReceive()), so the two stashed
        //      "Hello" messages are then echoed back to the client
        client.tell(new ConfigurationMessage(
                server,
                List.of(
                        new TextMessage("Hello"),
                        new TextMessage("Hi"),
                        new SleepMessage(),
                        new TextMessage("Hello"),
                        new TextMessage("Hello"),
                        new WakeupMessage()
                )
        ), ActorRef.noSender());

        // 4. Wait for user input before shutting down so asynchronous processing completes
        System.out.println("Press ENTER to terminate...");
        System.in.read();

        // 5. Stop ClientActor and ServerActor and release ActorSystem dispatcher threads
        system.terminate();
    }
}
