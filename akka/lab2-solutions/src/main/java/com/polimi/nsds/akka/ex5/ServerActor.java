package com.polimi.nsds.akka.ex5;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Supervised server actor managing a contact list that associates email addresses to names,
 * modified for Exercise 5 (Fault Tolerance &amp; Supervision).
 * <p>
 * <b>Fault Injection (Slide "Exercise 5"):</b>
 * <ul>
 *   <li>When a {@link PutMsg} arrives with {@code name.equals(FAULT_NAME)} ({@code "Fail!"}),
 *       {@link #onPut(PutMsg)} throws a generic {@link Exception} instead of storing the entry.</li>
 *   <li>Because {@link ServerActor} is created inside {@link SupervisorActor}'s context
 *       ({@code getContext().actorOf(...)}), the exception is caught by Akka and handled according
 *       to {@link SupervisorActor#supervisorStrategy()}:
 *       <ul>
 *         <li>With {@code SupervisorStrategy.resume()}: the existing {@link ServerActor} instance
 *             is kept alive and its {@code contacts} map is <b>preserved</b>;</li>
 *         <li>With {@code SupervisorStrategy.restart()}: the crashing {@link ServerActor} instance
 *             is replaced by a fresh instance (re-initializing {@code contacts} to an empty
 *             {@link HashMap}), so all previously stored contacts are <b>lost</b>, while its
 *             {@code ActorRef} and mailbox remain valid (Lecture 3, Slide 7 &amp; 12).</li>
 *       </ul>
 *   </li>
 * </ul>
 * </p>
 */
public class ServerActor extends AbstractActor {

    /**
     * Special contact name that triggers a simulated server fault upon receiving a {@link PutMsg}.
     */
    public static final String FAULT_NAME = "Fail!";

    // Private contact list: name -> email (reset to empty on restart, preserved on resume)
    private final Map<String, String> contacts = new HashMap<>();

    /**
     * Creates the server actor with an empty contact list.
     */
    public ServerActor() {
        // The contacts map is initialized empty in its field declaration
    }

    /**
     * Defines the match clauses for {@link PutMsg} and {@link GetMsg}.
     *
     * @return the {@link Receive} object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // Match PutMsg (which may throw Exception) and GetMsg
        return receiveBuilder()
                .match(PutMsg.class, this::onPut)
                .match(GetMsg.class, this::onGet)
                .build();
    }

    /**
     * Stores a contact atomically, or throws a generic {@link Exception} if the contact name is {@code "Fail!"}.
     *
     * @param msg the contact to store
     * @throws Exception if {@code msg.getName().equals("Fail!")}
     */
    void onPut(PutMsg msg) throws Exception {
        // Check if the message is a fault-injection command ("Fail!")
        if (FAULT_NAME.equals(msg.getName())) {
            System.out.println("[Server] Received PutMsg(\"Fail!\") -> Emulating a FAULT!");
            throw new Exception("Server fault triggered by PutMsg with name 'Fail!'");
        }
        // Normal operation: insert or overwrite the contact in the private map
        contacts.put(msg.getName(), msg.getEmail());
        System.out.println("[Server] Stored " + msg.getName() + " -> " + msg.getEmail());
    }

    /**
     * Looks up a contact by name and replies to {@code sender()} with a {@link ReplyMsg}.
     *
     * @param msg the lookup request
     */
    void onGet(GetMsg msg) {
        // Look up the email associated with the requested name
        final String email = contacts.get(msg.getName());
        // Build the reply: success if found, notFound otherwise
        final ReplyMsg reply = (email != null)
                ? ReplyMsg.success(msg.getName(), email)
                : ReplyMsg.notFound(msg.getName());
        // Log the lookup result on the server side
        System.out.println("[Server] Lookup " + msg.getName() + " -> " + (email != null ? email : "not found"));
        // Send the reply back to the requester (ClientActor or Ask future)
        sender().tell(reply, self());
    }

    /**
     * Lifecycle hook invoked on the crashing actor instance right before a {@code restart()}
     * (Lecture 3, Slide 12). Not invoked when the supervisor chooses {@code resume()}.
     *
     * @param reason  the exception that caused the restart
     * @param message the optional message that was being processed when the fault occurred
     */
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        System.out.print("[Server] Preparing to restart (contact list had " + contacts.size() + " entries)...");
    }

    /**
     * Lifecycle hook invoked on the newly created actor instance right after a {@code restart()}
     * (Lecture 3, Slide 12). Not invoked when the supervisor chooses {@code resume()}.
     *
     * @param reason the exception that caused the restart
     */
    @Override
    public void postRestart(Throwable reason) {
        System.out.println("...now restarted! (contact list now has " + contacts.size() + " entries)");
    }

    /**
     * Factory method returning the {@link Props} used to instantiate {@link ServerActor}.
     *
     * @return {@link Props} for {@link ServerActor}
     */
    public static Props props() {
        return Props.create(ServerActor.class);
    }
}
