package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * EXAM TEMPLATE 4: Client-Server Request/Reply (Lecture 2 - Slides 10 & 11)
 * <p>
 * Compares the two official ways an actor can wait for a reply from another actor:
 * <ol>
 *   <li><b>{@link AsyncClientActor} (Workaround: {@code become()} + {@code stash()})</b>:
 *       Non-blocking. Switches to {@code waitingForReply()}, stashes other incoming messages,
 *       and calls {@code unstashAll()} + {@code become(idle())} when {@code ReplyMsg} arrives.</li>
 *   <li><b>{@link BlockingClientAskActor} (Ask Pattern: {@code Patterns.ask()} + {@code Await.result()})</b>:
 *       Synchronous/blocking. Obtains a {@code Future<Object>} and blocks with {@code Await.result()}.</li>
 * </ol>
 * Notice that {@link ServerActor} is identical in both cases: it always replies via {@code sender().tell(reply, self())}.
 * </p>
 */
public class RequestReplyTemplate {

    // --- Protocol Messages ---
    public static class PutMsg implements Serializable {
        private final String key;
        private final String value;
        public PutMsg(String key, String value) { this.key = key; this.value = value; }
        public String getKey() { return key; }
        public String getValue() { return value; }
    }

    public static class GetMsg implements Serializable {
        private final String key;
        public GetMsg(String key) { this.key = key; }
        public String getKey() { return key; }
    }

    public static class ReplyMsg implements Serializable {
        private final String key;
        private final String value; // null if not found
        public ReplyMsg(String key, String value) { this.key = key; this.value = value; }
        public String getKey() { return key; }
        public String getValue() { return value; }
    }

    // =========================================================================
    // 1. SERVER ACTOR (Replies with sender().tell(reply, self()))
    // =========================================================================
    public static class ServerActor extends AbstractActor {
        private final Map<String, String> store = new HashMap<>();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(PutMsg.class, msg -> store.put(msg.getKey(), msg.getValue()))
                    .match(GetMsg.class, msg -> {
                        String val = store.get(msg.getKey());
                        sender().tell(new ReplyMsg(msg.getKey(), val), self());
                    })
                    .build();
        }

        public static Props props() {
            return Props.create(ServerActor.class);
        }
    }

    // =========================================================================
    // 2A. NON-BLOCKING CLIENT (Workaround: become() + stash() + unstashAll())
    // =========================================================================
    public static class AsyncClientActor extends AbstractActorWithStash {
        private final ActorRef server;

        public AsyncClientActor(ActorRef server) {
            this.server = server;
        }

        @Override
        public Receive createReceive() {
            return idle();
        }

        private Receive idle() {
            return receiveBuilder()
                    .match(PutMsg.class, msg -> server.tell(msg, self()))
                    .match(GetMsg.class, msg -> {
                        server.tell(msg, self());
                        // Switch behavior to wait exclusively for ReplyMsg
                        getContext().become(waitingForReply());
                    })
                    .build();
        }

        private Receive waitingForReply() {
            return receiveBuilder()
                    .match(ReplyMsg.class, reply -> {
                        System.out.println("[AsyncClient] Reply for '" + reply.getKey() + "': " + reply.getValue());
                        // Restore stashed messages and return to normal behavior
                        unstashAll();
                        getContext().become(idle());
                    })
                    .matchAny(msg -> stash()) // Stash everything else while waiting
                    .build();
        }

        public static Props props(ActorRef server) {
            return Props.create(AsyncClientActor.class, () -> new AsyncClientActor(server));
        }
    }

    // =========================================================================
    // 2B. SYNCHRONOUS CLIENT (Ask Pattern: Patterns.ask() + Await.result())
    // =========================================================================
    public static class BlockingClientAskActor extends AbstractActor {
        private static final long TIMEOUT_MS = 5000;
        private final ActorRef server;

        public BlockingClientAskActor(ActorRef server) {
            this.server = server;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(PutMsg.class, msg -> server.tell(msg, self()))
                    .match(GetMsg.class, this::onGetSync)
                    .build();
        }

        private void onGetSync(GetMsg msg) {
            try {
                Future<Object> future = Patterns.ask(server, msg, TIMEOUT_MS);
                ReplyMsg reply = (ReplyMsg) Await.result(future, Duration.create(5, TimeUnit.SECONDS));
                System.out.println("[BlockingClientAsk] Reply for '" + reply.getKey() + "': " + reply.getValue());
            } catch (Exception e) {
                System.err.println("[BlockingClientAsk] Timeout or error: " + e.getMessage());
            }
        }

        public static Props props(ActorRef server) {
            return Props.create(BlockingClientAskActor.class, () -> new BlockingClientAskActor(server));
        }
    }

    // --- Runnable Demo ---
    public static void main(String[] args) throws IOException {
        final ActorSystem sys = ActorSystem.create("RequestReplySystem");
        final ActorRef server = sys.actorOf(ServerActor.props(), "server");
        final ActorRef asyncClient = sys.actorOf(AsyncClientActor.props(server), "asyncClient");
        final ActorRef askClient = sys.actorOf(BlockingClientAskActor.props(server), "askClient");

        asyncClient.tell(new PutMsg("Alice", "alice@polimi.it"), ActorRef.noSender());
        asyncClient.tell(new GetMsg("Alice"), ActorRef.noSender());
        asyncClient.tell(new GetMsg("Bob"), ActorRef.noSender());

        askClient.tell(new GetMsg("Alice"), ActorRef.noSender());

        System.out.println("Press ENTER to terminate...");
        System.in.read();
        sys.terminate();
    }
}
