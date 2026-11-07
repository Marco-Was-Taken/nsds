package com.polimi.nsds.akka.ex5;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.IOException;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Main entry point and test driver for Exercise 5 (Fault-Tolerant Contact List with Supervision).
 *
 * <h2>Exercise 5 Requirements (Lab 2, Slide 3)</h2>
 * <ul>
 *   <li>Modify the solution to Exercise 3 so that {@link ServerActor} fails every time a
 *       {@link PutMsg} is received with name {@code "Fail!"} (raising a generic {@link Exception}).</li>
 *   <li>Create a {@link SupervisorActor} that manages the fault.</li>
 *   <li>Verify that using a <b>{@code resume}</b> strategy preserves the contact list, whereas a
 *       <b>{@code restart}</b> strategy effectively resets the contact list.</li>
 * </ul>
 *
 * <h2>Step-by-Step Execution Trace</h2>
 * <p>
 * By default (no arguments), this program runs <b>both scenarios</b> sequentially (first
 * {@code RESUME}, then {@code RESTART}) so you can immediately compare their outputs; you can also
 * pass {@code "resume"} or {@code "restart"} as a command-line argument to run a single strategy.
 * </p>
 * <ol>
 *   <li><b>Spawning {@link ServerActor} under {@link SupervisorActor} (Lecture 3, Slide 13):</b>
 *       {@code main} creates {@link SupervisorActor} at top level and sends it
 *       {@code Props.create(ServerActor.class)} via {@code ask(supervisor, ..., 5000)}.
 *       {@link SupervisorActor} calls {@code getContext().actorOf(props, "serverActor")} and replies
 *       with the child's {@link ActorRef}. Because {@link SupervisorActor} created {@link ServerActor}
 *       in its own context, {@link SupervisorActor} is its supervisor.</li>
 *   <li><b>Inserting Initial Contacts ({@code Alice} and {@code Bob}):</b>
 *       {@link ClientActor} sends {@code PutMsg("Alice", "alice@polimi.it")} and
 *       {@code PutMsg("Bob", "bob@polimi.it")}. {@link ServerActor} stores both entries in its
 *       private {@code HashMap}. A lookup {@code GetMsg("Alice")} confirms {@code Alice -> alice@polimi.it}.</li>
 *   <li><b>Triggering the Fault ({@code PutMsg("Fail!", "crash@polimi.it")}):</b>
 *       {@link ClientActor} sends {@code PutMsg("Fail!", "crash@polimi.it")}. Inside
 *       {@code ServerActor.onPut}, {@code "Fail!".equals(msg.getName())} is {@code true}, so
 *       {@link ServerActor} throws {@code new Exception("Server fault triggered by PutMsg with name 'Fail!'")}.</li>
 *   <li><b>Supervision Decision &amp; Subsequent Lookup ({@code GetMsg("Alice")} and {@code GetMsg("Bob")}):</b>
 *       <ul>
 *         <li><b>Case A — {@code SupervisorStrategy.resume()}:</b>
 *             {@link SupervisorActor} instructs Akka to resume the existing {@link ServerActor}
 *             instance. Neither {@code preRestart} nor {@code postRestart} is called, and the
 *             {@code contacts} map is <b>preserved</b>. The subsequent {@code GetMsg("Alice")} and
 *             {@code GetMsg("Bob")} still return {@code alice@polimi.it} and {@code bob@polimi.it}!</li>
 *         <li><b>Case B — {@code SupervisorStrategy.restart()}:</b>
 *             {@link SupervisorActor} instructs Akka to restart {@link ServerActor}. Akka calls
 *             {@code preRestart} on the old instance, instantiates a fresh {@link ServerActor}
 *             with a new empty {@code HashMap}, and calls {@code postRestart}. Crucially, the
 *             server's {@link ActorRef} and mailbox do <b>not</b> change (Lecture 3, Slide 7), so
 *             the subsequent {@code GetMsg("Alice")} and {@code GetMsg("Bob")} are processed by the
 *             new instance and return {@code "not found"} because the contact list was <b>reset</b>!</li>
 *       </ul>
 *   </li>
 * </ol>
 */
public class ContactListApp {

    /**
     * Runs the Exercise 5 test scenario for a given supervision strategy ({@code resume} vs {@code restart}).
     *
     * @param systemName name of the {@link ActorSystem}
     * @param useRestart {@code false} to test {@code SupervisorStrategy.resume()},
     *                   {@code true} to test {@code SupervisorStrategy.restart()}
     * @throws Exception if asking the supervisor for the child reference fails or times out
     */
    private static void runScenario(String systemName, boolean useRestart) throws Exception {
        final String modeLabel = useRestart ? "RESTART STRATEGY (resets contact list)" : "RESUME STRATEGY (preserves contact list)";
        System.out.println("\n================================================================================");
        System.out.println(" RUNNING EXERCISE 5 WITH: " + modeLabel);
        System.out.println("================================================================================");

        // 1. Create the ActorSystem and the top-level SupervisorActor configured with the chosen strategy
        final Duration timeout = Duration.create(5, SECONDS);
        final ActorSystem sys = ActorSystem.create(systemName);
        final ActorRef supervisor = sys.actorOf(SupervisorActor.props(useRestart), "supervisor");

        // 2. Ask the SupervisorActor to instantiate ServerActor as its supervised child (Lecture 3, Slide 13)
        final Future<Object> waitingForServer = ask(supervisor, Props.create(ServerActor.class), 5000);
        final ActorRef server = (ActorRef) waitingForServer.result(timeout, null);

        // 3. Create the ClientActor bound to the supervised ServerActor's ActorRef
        final ActorRef client = sys.actorOf(ClientActor.props(server), "clientActor");

        // 4. Populate the contact list with Alice and Bob and verify Alice is present before the fault
        client.tell(new PutMsg("Alice", "alice@polimi.it"), ActorRef.noSender());
        client.tell(new PutMsg("Bob", "bob@polimi.it"), ActorRef.noSender());
        client.tell(new GetMsg("Alice"), ActorRef.noSender());

        // 5. Send PutMsg with name "Fail!" -> ServerActor throws Exception -> SupervisorActor handles it
        client.tell(new PutMsg(ServerActor.FAULT_NAME, "crash@polimi.it"), ActorRef.noSender());

        // 6. Query Alice and Bob after the fault:
        //    - Under RESUME: Alice and Bob are still found in the contact list
        //    - Under RESTART: Alice and Bob are no longer found (contact list was reset),
        //      and ServerActor's ActorRef & mailbox still work seamlessly for new operations!
        client.tell(new GetMsg("Alice"), ActorRef.noSender());
        client.tell(new GetMsg("Bob"), ActorRef.noSender());

        // 7. Insert a new contact after the fault to prove the supervised ServerActor is still operational
        client.tell(new PutMsg("Charlie", "charlie@polimi.it"), ActorRef.noSender());
        client.tell(new GetMsg("Charlie"), ActorRef.noSender());

        // Allow asynchronous messages to complete before shutting down this scenario's ActorSystem
        Thread.sleep(1000);
        sys.terminate();
        Thread.sleep(300);
    }

    /**
     * Main entry point: runs Exercise 5 with {@code resume} and/or {@code restart} strategy.
     *
     * @param args optional: {@code "resume"} or {@code "restart"} (runs both if omitted)
     * @throws IOException if reading from standard input fails
     */
    public static void main(String[] args) throws IOException {
        try {
            if (args.length > 0 && args[0].equalsIgnoreCase("resume")) {
                runScenario("Ex5ResumeSystem", false);
            } else if (args.length > 0 && args[0].equalsIgnoreCase("restart")) {
                runScenario("Ex5RestartSystem", true);
            } else {
                // Default: run both strategies back-to-back to verify the difference
                runScenario("Ex5ResumeSystem", false);
                runScenario("Ex5RestartSystem", true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
