package com.polimi.akka.template;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

/**
 * Entry point of the template project.
 * <p>
 * It follows the steps shown in the slides ("Create actors" / "Send messages to actors"):
 * <ol>
 *   <li>instantiate an {@link ActorSystem};</li>
 *   <li>create an actor from its Props with {@code sys.actorOf(props, name)}, obtaining an {@link ActorRef};</li>
 *   <li>send an asynchronous message with {@code tell()};</li>
 *   <li>wait (here: until ENTER is pressed) and terminate the system.</li>
 * </ol>
 * </p>
 */
public class MainTemplate {

    /**
     * Starts the local actor system, sends one message and terminates the system.
     *
     * @param args command-line arguments (not used)
     * @throws IOException if reading from standard input fails
     */
    public static void main(String[] args) throws IOException {
        // Instantiate the local ActorSystem
        final ActorSystem sys = ActorSystem.create("LocalTemplateSystem");

        // Create the actor; actorOf returns an ActorRef that stays valid for the actor's whole lifecycle
        final ActorRef templateActor = sys.actorOf(TemplateActor.props(), "templateActor");

        // Asynchronous, fire-and-forget send: no sender because no reply is expected
        templateActor.tell(new TemplateMessage("Hello Akka Classic!"), ActorRef.noSender());

        // Wait for the message to be processed before terminating (same approach as the slides)
        System.out.println("[MainTemplate] Press ENTER to terminate the system...");
        System.in.read();

        // Terminate the ActorSystem: all actors are stopped (postStop() is invoked)
        sys.terminate();
    }
}
