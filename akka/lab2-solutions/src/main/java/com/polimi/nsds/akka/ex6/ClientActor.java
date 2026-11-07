package com.polimi.nsds.akka.ex6;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Client actor for Exercise 6 (Asynchronous Server Processing with Scheduler, Become &amp; Stash).
 * <p>
 * Accepts:
 * <ul>
 *   <li>{@link ConfigurationMessage}: stores the {@link ServerActor} reference and sends the
 *       configured list of {@link Message}s to the server, passing {@code getSelf()} as sender;</li>
 *   <li>{@link TextMessage}: prints the reply received from {@link ServerActor} once the server's
 *       asynchronous processing timer completes.</li>
 * </ul>
 * </p>
 */
public class ClientActor extends AbstractActor {

    // Reference to the target ServerActor, initialized upon receiving ConfigurationMessage
    private ActorRef server;

    /**
     * Defines the message handlers for incoming replies ({@link TextMessage}) and initial
     * configuration ({@link ConfigurationMessage}).
     *
     * @return the {@link Receive} behavior of this client actor
     */
    @Override
    public Receive createReceive() {
        // Match replies from the server and initial configuration from main
        return receiveBuilder()
                .match(TextMessage.class, this::onTextMessage)
                .match(ConfigurationMessage.class, this::onConfigurationMessage)
                .build();
    }

    /**
     * Handles a reply {@link TextMessage} returned by {@link ServerActor} after processing.
     *
     * @param message the reply text message
     */
    public void onTextMessage(TextMessage message) {
        // Print the received message payload on standard output
        System.out.println("Received Message from ClientActor: " + message.getText());
    }

    /**
     * Factory method returning the {@link Props} used to instantiate {@link ClientActor}.
     *
     * @return {@link Props} for {@link ClientActor}
     */
    public static Props props() {
        return Props.create(ClientActor.class);
    }

    /**
     * Handles the initial {@link ConfigurationMessage} by saving the server reference and
     * dispatching all configured messages to the server in FIFO order.
     *
     * @param message the configuration message containing the server reference and message list
     */
    public void onConfigurationMessage(ConfigurationMessage message) {
        // Save the target server reference
        server = message.getServer();

        // Dispatch all configured messages to the server with getSelf() as sender
        for (Message textMessage : message.getMessages()) {
            server.tell(textMessage, getSelf());
        }
    }
}
