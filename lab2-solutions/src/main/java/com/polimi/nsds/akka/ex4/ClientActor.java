package com.polimi.nsds.akka.ex4;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Client actor for Exercise 4 (Sleep/Wakeup Echo Server).
 * <p>
 * The actor accepts two types of messages:
 * <ul>
 *   <li>{@link ConfigurationMessage}: stores the {@link ServerActor} reference and sends the
 *       configured list of messages ({@link TextMessage}, {@link SleepMessage}, {@link WakeupMessage})
 *       to the server in FIFO order, passing {@code getSelf()} as sender so that echoed replies return
 *       to this client's mailbox;</li>
 *   <li>{@link TextMessage}: prints the echoed text reply received back from {@link ServerActor}.</li>
 * </ul>
 * </p>
 */
public class ClientActor extends AbstractActor {

    // Reference to the target ServerActor, initialized upon receiving ConfigurationMessage
    private ActorRef server;

    /**
     * Defines the message handlers for echoed {@link TextMessage}s and initial {@link ConfigurationMessage}.
     *
     * @return the {@link Receive} behavior of this client actor
     */
    @Override
    public Receive createReceive() {
        // Match incoming echoed text messages and initial configuration
        return receiveBuilder()
                .match(TextMessage.class, this::onTextMessage)
                .match(ConfigurationMessage.class, this::onConfigurationMessage)
                .build();
    }

    /**
     * Handles an echoed {@link TextMessage} sent back by {@link ServerActor}.
     *
     * @param message the echoed text message
     */
    public void onTextMessage(TextMessage message) {
        // Print the echoed message payload on standard output
        System.out.println("Received Message from ClientActor: " + message.getText());
    }

    /**
     * Handles the initial {@link ConfigurationMessage} by saving the server reference and
     * dispatching all configured messages to the server in sequence.
     *
     * @param message the configuration message containing the server reference and message list
     */
    public void onConfigurationMessage(ConfigurationMessage message) {
        // Save the target server reference
        server = message.getServer();

        // Send all messages in FIFO order from this single sender (getSelf())
        for (Message textMessage : message.getMessages()) {
            server.tell(textMessage, getSelf());
        }
    }

    /**
     * Factory method returning the {@link Props} used to instantiate {@link ClientActor}.
     *
     * @return {@link Props} for {@link ClientActor}
     */
    public static Props props() {
        return Props.create(ClientActor.class);
    }
}
