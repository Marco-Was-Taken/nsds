package com.polimi.akka.template;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * EXAM TEMPLATE 6: Remote Distribution over TCP (Lecture 2 - Slides 13–17)
 * <p>
 * Shows how to configure a remote {@link ActorSystem} from a configuration file and
 * how a client looks up a remote actor by address using {@link ActorSelection}.
 * </p>
 */
public class RemoteDistributionTemplate {

    // =========================================================================
    // 1. REMOTE SERVER BOOTSTRAP (Lecture 2, Slide 16)
    // =========================================================================
    public static void startServer() {
        // Load HOCON configuration file specifying remote provider, hostname, and port
        Config conf = ConfigFactory.parseFile(new File("src/main/resources/remote.conf"));
        ActorSystem sys = ActorSystem.create("Server", conf);
        sys.actorOf(RequestReplyTemplate.ServerActor.props(), "serverActor");
    }

    // =========================================================================
    // 2. REMOTE CLIENT ACTOR USING ActorSelection (Lecture 2, Slide 17)
    // =========================================================================
    public static class RemoteClientActor extends AbstractActor {

        // Format: akka.tcp://<ActorSystemName>@<Hostname>:<Port>/user/<ActorName>
        private static final String SERVER_ADDR =
                "akka.tcp://Server@127.0.0.1:6123/user/serverActor";

        private ActorSelection server;

        @Override
        public void preStart() {
            // Retrieve the remote server by address
            this.server = getContext().actorSelection(SERVER_ADDR);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(RequestReplyTemplate.PutMsg.class, msg -> {
                        // Everything else remains the same as local tell()!
                        server.tell(msg, self());
                    })
                    .match(RequestReplyTemplate.GetMsg.class, msg -> {
                        server.tell(msg, self());
                    })
                    .match(RequestReplyTemplate.ReplyMsg.class, reply -> {
                        System.out.println("[RemoteClient] Received reply: " + reply.getValue());
                    })
                    .build();
        }

        public static Props props() {
            return Props.create(RemoteClientActor.class);
        }
    }
}
