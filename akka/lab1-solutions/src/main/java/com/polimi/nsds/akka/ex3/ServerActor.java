package com.polimi.nsds.akka.ex3;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;

/**
 * Server actor managing a contact list that associates email addresses to names (Exercise 3).
 * <p>
 * It processes two types of messages:
 * <ul>
 *   <li>{@link PutMsg}: adds (or overwrites) a contact in the list; no reply is sent;</li>
 *   <li>{@link GetMsg}: reads the address of a contact given its name and replies to the
 *       sender with a {@link ReplyMsg}.</li>
 * </ul>
 * The reply uses {@code sender().tell(reply, self())}, exactly as shown in the slides
 * ("Replying to messages" and "Ask Pattern"). The sender reference works both when the
 * request comes from another actor (the reply is enqueued in its mailbox) and when it comes
 * from the Ask pattern (the reply completes the returned future).
 * </p>
 * <p>
 * A plain {@link HashMap} is enough: the map is private state and messages are processed
 * one at a time, so concurrent modifications are impossible.
 * </p>
 */
public class ServerActor extends AbstractActor {

    // Private contact list: name -> email
    private final Map<String, String> contacts = new HashMap<>();

    /**
     * Creates the server actor with an empty contact list.
     */
    public ServerActor() {
        // The map is already initialized in its declaration
    }

    /**
     * Defines the match clauses for PutMsg and GetMsg.
     *
     * @return the Receive object with the match clauses
     */
    @Override
    public Receive createReceive() {
        // One clause per request class
        return receiveBuilder()
                .match(PutMsg.class, this::onPut)
                .match(GetMsg.class, this::onGet)
                .build();
    }

    /**
     * Stores a contact atomically.
     *
     * @param msg the contact to store
     */
    void onPut(PutMsg msg) {
        // Insert or overwrite the contact
        contacts.put(msg.getName(), msg.getEmail());
        // Log the operation on the server side
        System.out.println("[Server] Stored " + msg.getName() + " -> " + msg.getEmail());
    }

    /**
     * Looks up a contact and replies to the sender of the request.
     *
     * @param msg the lookup request
     */
    void onGet(GetMsg msg) {
        // Look up the email associated with the requested name
        final String email = contacts.get(msg.getName());
        // Build the reply: found or not found
        final ReplyMsg reply = (email != null)
                ? ReplyMsg.success(msg.getName(), email)
                : ReplyMsg.notFound(msg.getName());
        // Log the operation on the server side
        System.out.println("[Server] Lookup " + msg.getName() + " -> " + (email != null ? email : "not found"));
        // Reply to whoever sent the GetMsg (slide: sender().tell(reply, self()))
        sender().tell(reply, self());
    }

    /**
     * Factory method returning the Props used to create this actor.
     *
     * @return Props for ServerActor
     */
    static Props props() {
        // Same form as the slides
        return Props.create(ServerActor.class);
    }
}
