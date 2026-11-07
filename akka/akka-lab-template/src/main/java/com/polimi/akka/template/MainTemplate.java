package com.polimi.akka.template;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * EXAM TEMPLATE: Main Bootstrapper (tell, ExecutorService, and Ask)
 */
public class MainTemplate {

    private static final int NUM_THREADS = 16;
    private static final int NUM_MESSAGES = 10;
    private static final long TIMEOUT_MS = 5000;

    public static void main(String[] args) throws IOException {
        // 1. Instantiate the ActorSystem
        final ActorSystem sys = ActorSystem.create("System");

        // 2. Create an actor using its Props factory method
        final ActorRef actor = sys.actorOf(TemplateActor.props("worker-1"), "templateActor");

        // 3. Fire-and-forget asynchronous send (no reply expected -> ActorRef.noSender())
        actor.tell(new TemplateMessage("Single async message", 0), ActorRef.noSender());
        actor.tell(new TemplateMessage("Negative code message", -1), ActorRef.noSender());

        // 4. Multi-threaded stress test (Lecture 1 pattern)
        final ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            final int idx = i;
            exec.submit(() -> actor.tell(new TemplateMessage("Parallel msg #" + idx, idx), ActorRef.noSender()));
        }

        // 5. Synchronous request-reply from main using Patterns.ask + Await.result (Lecture 2/3 pattern)
        try {
            Future<Object> future = Patterns.ask(actor, new TemplateMessage("Sync request"), TIMEOUT_MS);
            TemplateMessage reply = (TemplateMessage) Await.result(future, Duration.create(5, TimeUnit.SECONDS));
            System.out.println("[MainTemplate] Received synchronous reply: " + reply);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("[MainTemplate] Press ENTER to terminate...");
        System.in.read();

        // 6. Clean shutdown
        exec.shutdown();
        sys.terminate();
    }
}

