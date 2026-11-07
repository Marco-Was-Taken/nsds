package com.polimi.nsds.akka.ex3;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

/**
 * Main entry point and test driver for Exercise 3 (Client/Server Contact Directory)
 * running inside a single local {@link ActorSystem}.
 *
 * <h2>Architectural Overview</h2>
 * <p>
 * The program creates a {@link ServerActor} (which stores a private {@code HashMap<String, String>}
 * mapping contact names to email addresses) and a client actor ({@link ClientActor} by default, or
 * {@link ClientAskActor} when launched with the {@code "ask"} command-line argument).
 * The {@code main} thread then dispatches a sequence of 6 commands ({@link PutMsg} and {@link GetMsg})
 * asynchronously to the client actor using {@code tell()}.
 * </p>
 *
 * <h2>Detailed Step-by-Step Execution Trace</h2>
 *
 * <h3>Phase 1: ActorSystem Bootstrap and Actor Creation</h3>
 * <ol>
 *   <li><b>Client mode selection ({@code useAsk}):</b>
 *       Without arguments ({@code useAsk == false}), the program instantiates {@link ClientActor},
 *       which implements the non-blocking workaround based on {@code getContext().become(...)} +
 *       {@code stash()} / {@code unstashAll()} (Lecture 2, Slide 10: <i>"Motivation for the Ask Pattern"</i>).
 *       With {@code "ask"} ({@code useAsk == true}), it instantiates {@link ClientAskActor}, which
 *       implements the synchronous blocking Ask Pattern via {@code Patterns.ask(...)} +
 *       {@code Await.result(...)} (Lecture 2, Slide 11: <i>"Ask Pattern"</i>).</li>
 *   <li><b>{@code ActorSystem.create("System")}:</b>
 *       Initializes the local Akka container (Lecture 1, Slide 17), including the dispatcher and thread
 *       pools that schedule and execute actor message handlers.</li>
 *   <li><b>Creation of {@link ServerActor}:</b>
 *       {@code ServerActor.props()} invokes {@code Props.create(ServerActor.class)}.
 *       {@code sys.actorOf(...)} instantiates the actor, assigns it a dedicated mailbox, and returns its
 *       immutable {@link ActorRef} ({@code akka://System/user/serverActor}). Inside {@link ServerActor},
 *       the contact list is a plain {@code HashMap<String, String>}: neither {@code ConcurrentHashMap}
 *       nor {@code synchronized} blocks are needed because Akka processes messages one at a time per actor
 *       instance (Lecture 1, Slide 13).</li>
 *   <li><b>Creation of the Client actor with the Server {@link ActorRef}:</b>
 *       {@code ClientActor.props(server)} invokes {@code Props.create(ClientActor.class, server)}, passing
 *       the server's {@link ActorRef} to the constructor {@code ClientActor(ActorRef server)} (Lecture 1,
 *       Slide 19: {@code ActorRef}s can be passed to inform other actors). When {@link ClientActor} starts,
 *       Akka invokes {@code createReceive()}, which returns the initial {@code idle()} behavior (accepting
 *       {@link PutMsg} and {@link GetMsg}).</li>
 * </ol>
 *
 * <h3>Phase 2: Enqueuing the Command Sequence from {@code main} to {@code client}</h3>
 * <p>
 * The {@code main} thread performs 6 consecutive, non-blocking {@code client.tell(...)} calls.
 * Because all 6 messages originate from the <b>same sender</b> (the {@code main} thread), Akka's default
 * FIFO mailbox policy (Lecture 1, Slide 23: <i>"FIFO order from a single sender"</i>) guarantees that they
 * are enqueued into {@code clientActor}'s mailbox in exact submission order:
 * </p>
 * <ol>
 *   <li>{@code PutMsg("Alice", "alice@polimi.it")}</li>
 *   <li>{@code PutMsg("Bob", "bob@polimi.it")}</li>
 *   <li>{@code GetMsg("Alice")}</li>
 *   <li>{@code GetMsg("Diana")}</li>
 *   <li>{@code PutMsg("Bob", "bob.new@polimi.it")}</li>
 *   <li>{@code GetMsg("Bob")}</li>
 * </ol>
 * <p>
 * {@code ActorRef.noSender()} is passed because the {@code main} thread is not an actor and expects no
 * reply; {@code clientActor} will act as the actual sender toward {@code serverActor} by passing {@code self()}.
 * </p>
 *
 * <h3>Phase 3: Message-by-Message Trace (Case A: {@link ClientActor} with {@code become} + {@code stash})</h3>
 * <ol>
 *   <li><b>Processing {@code PutMsg("Alice", "alice@polimi.it")} and {@code PutMsg("Bob", "bob@polimi.it")}:</b>
 *       <ul>
 *         <li>{@link ClientActor} is in its {@code idle()} behavior.</li>
 *         <li>The clause {@code .match(PutMsg.class, this::onPut)} invokes {@code onPut(PutMsg msg)},
 *             which logs {@code [Client] Put Alice -> alice@polimi.it} and forwards the message to the server
 *             via {@code server.tell(msg, self())}. Since no reply is expected for a {@code PutMsg}, the
 *             behavior remains {@code idle()}.</li>
 *         <li>Immediately after, {@link ClientActor} dequeues {@code PutMsg("Bob", "bob@polimi.it")}, logs
 *             {@code [Client] Put Bob -> bob@polimi.it}, and forwards it via {@code server.tell(msg, self())}.</li>
 *         <li>On the server side, {@code ServerActor.onPut} receives the two {@link PutMsg}s in FIFO order and
 *             inserts {@code "Alice" -> "alice@polimi.it"} and {@code "Bob" -> "bob@polimi.it"} into its private
 *             {@code HashMap}.</li>
 *       </ul>
 *   </li>
 *   <li><b>Processing {@code GetMsg("Alice")} and switching to {@code waitingForReply()} + {@code stash()}:</b>
 *       <ul>
 *         <li>{@link ClientActor} (still in {@code idle()}) dequeues the 3rd message: {@code GetMsg("Alice")}.</li>
 *         <li>{@code .match(GetMsg.class, this::onGet)} runs {@code onGet(GetMsg msg)}:
 *             (a) logs {@code [Client] Get Alice};
 *             (b) sends {@code server.tell(msg, self())} (passing {@code self()} is essential so {@code serverActor}
 *             knows to send the reply back to {@code clientActor});
 *             (c) calls {@code getContext().become(waitingForReply())} (Lecture 1, Slides 24-25; Lecture 2, Slide 10).</li>
 *         <li>From this moment, {@link ClientActor}'s active behavior is {@code waitingForReply()}, which only
 *             matches {@code ReplyMsg.class} ({@code this::onReply}) and stashes everything else via
 *             {@code .matchAny(msg -> stash())}.</li>
 *         <li>While the server's reply for {@code "Alice"} is in flight, the remaining commands already in
 *             {@code clientActor}'s mailbox ({@code GetMsg("Diana")}, {@code PutMsg("Bob", "bob.new@polimi.it")},
 *             {@code GetMsg("Bob")}) do not match {@code ReplyMsg.class}: they hit {@code .matchAny(msg -> stash())}
 *             and are set aside in the actor's internal stash in FIFO order without being sent to the server prematurely.</li>
 *       </ul>
 *   </li>
 *   <li><b>Receiving {@code ReplyMsg} for {@code "Alice"} and calling {@code unstashAll()}:</b>
 *       <ul>
 *         <li>{@code ServerActor.onGet} processes {@code GetMsg("Alice")}, looks up {@code "Alice"} in
 *             {@code contacts}, finds {@code "alice@polimi.it"}, builds {@code ReplyMsg.success("Alice", "alice@polimi.it")}
 *             ({@code found = true}), and replies via {@code sender().tell(reply, self())} (Lecture 1, Slide 26).</li>
 *         <li>{@link ClientActor} (in {@code waitingForReply()}) receives {@link ReplyMsg} and runs {@code onReply}:
 *             (a) sees {@code reply.isFound() == true} and prints {@code [Client] REPLY: Alice -> alice@polimi.it};
 *             (b) calls {@code unstashAll()}, which extracts all stashed messages ({@code GetMsg("Diana")},
 *             {@code PutMsg("Bob", ...)}, {@code GetMsg("Bob")}) in the exact order they were stashed and prepends
 *             them back to the front of the mailbox (Lecture 2, Slide 9);
 *             (c) calls {@code getContext().become(idle())} to return to normal command processing.</li>
 *       </ul>
 *   </li>
 *   <li><b>Processing {@code GetMsg("Diana")} (missing contact):</b>
 *       <ul>
 *         <li>Back in {@code idle()}, {@link ClientActor} dequeues {@code GetMsg("Diana")} from the front of its
 *             mailbox, forwards it via {@code server.tell(msg, self())}, and switches back to {@code waitingForReply()}.</li>
 *         <li>Any subsequent messages ({@code PutMsg("Bob", ...)} and {@code GetMsg("Bob")}) are stashed again.</li>
 *         <li>{@code ServerActor.onGet} looks up {@code "Diana"}, gets {@code null}, builds
 *             {@code ReplyMsg.notFound("Diana")} ({@code found = false}), and sends it via
 *             {@code sender().tell(reply, self())}.</li>
 *         <li>{@code ClientActor.onReply} sees {@code reply.isFound() == false}, prints
 *             {@code [Client] REPLY: Error: Contact 'Diana' not found in contact list}, calls {@code unstashAll()},
 *             and switches back to {@code idle()}.</li>
 *       </ul>
 *   </li>
 *   <li><b>Overwriting {@code "Bob"} ({@code PutMsg("Bob", "bob.new@polimi.it")}) and querying {@code GetMsg("Bob")}:</b>
 *       <ul>
 *         <li>Back in {@code idle()}, {@link ClientActor} dequeues {@code PutMsg("Bob", "bob.new@polimi.it")},
 *             forwards it to {@code server} via {@code server.tell(msg, self())}, and stays in {@code idle()}.</li>
 *         <li>Next, it dequeues the final command {@code GetMsg("Bob")}, forwards it via {@code server.tell(msg, self())},
 *             and switches to {@code waitingForReply()}.</li>
 *         <li>Because {@link ClientActor} sent {@code PutMsg("Bob", "bob.new@polimi.it")} before {@code GetMsg("Bob")}
 *             to the same receiver ({@link ServerActor}), FIFO ordering between the sender-receiver pair guarantees
 *             that {@link ServerActor} updates the map entry to {@code "bob.new@polimi.it"} before reading it.</li>
 *         <li>{@code ClientActor.onReply} receives the final reply, prints
 *             {@code [Client] REPLY: Bob -> bob.new@polimi.it}, calls {@code unstashAll()}, and returns to {@code idle()}.</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h3>Phase 4: Comparison with Case B ({@link ClientAskActor} with {@code "ask"} argument)</h3>
 * <p>
 * When launched with {@code "ask"}, {@link ClientAskActor} extends plain {@code AbstractActor} (without
 * {@code Stash} or {@code become()}). Inside {@code onGet(GetMsg msg)}:
 * </p>
 * <pre>{@code
 * final Future<Object> future = Patterns.ask(server, msg, TIMEOUT_MS);
 * final ReplyMsg reply = (ReplyMsg) Await.result(future, timeout);
 * }</pre>
 * <ul>
 *   <li>{@code Patterns.ask(server, msg, TIMEOUT_MS)} spawns a temporary internal actor that acts as
 *       {@code sender()} toward {@link ServerActor} and returns a {@code Future<Object>}.</li>
 *   <li>{@link ServerActor} replies in the exact same way ({@code sender().tell(reply, self())}), completing
 *       the {@code Future}.</li>
 *   <li>{@code Await.result(future, timeout)} blocks the execution of {@code onGet} on the current thread
 *       until the reply arrives (or the 5-second timeout expires). Because {@code onGet} does not return
 *       until the reply is printed, subsequent messages automatically wait in {@code clientActor}'s mailbox
 *       without needing {@code stash()}.</li>
 * </ul>
 *
 * <h3>Phase 5: System Termination</h3>
 * <p>
 * Because {@code client.tell(...)} is asynchronous, {@code System.in.read()} keeps the JVM alive until the
 * user presses {@code ENTER}; otherwise {@code sys.terminate()} would stop the actors before message exchange
 * completes. Pressing {@code ENTER} invokes {@code sys.terminate()}, stopping {@code clientActor} and
 * {@code serverActor} and shutting down the {@link ActorSystem} thread pool.
 * </p>
 */
public class ContactListApp {

    /**
     * Creates server and client, runs insertions and lookups, waits for ENTER and terminates.
     *
     * @param args optional: "ask" to use the Ask-pattern client
     * @throws IOException if reading from standard input fails
     */
    public static void main(String[] args) throws IOException {
        // =========================================================================================
        // PHASE 1: ActorSystem bootstrap and creation of ServerActor and ClientActor / ClientAskActor
        // =========================================================================================

        // 1. Select the client implementation from command-line arguments:
        //    - default (no args): ClientActor (non-blocking become() + stash()/unstashAll() workaround, Lecture 2 Slide 10)
        //    - "ask":             ClientAskActor (blocking Patterns.ask() + Await.result() pattern, Lecture 2 Slide 11)
        final boolean useAsk = args.length > 0 && args[0].equalsIgnoreCase("ask");

        // 2. Instantiate the single local ActorSystem ("System") that hosts both actors (Lecture 1 Slide 17)
        final ActorSystem sys = ActorSystem.create("System");

        // 3. Create the ServerActor via its static factory ServerActor.props() -> Props.create(ServerActor.class).
        //    actorOf(...) registers the actor at path "akka://System/user/serverActor", allocates its mailbox,
        //    and returns its immutable ActorRef. Internally, ServerActor initializes an empty HashMap<String, String>
        //    (no locks or ConcurrentHashMap needed because Akka processes one message at a time per actor).
        final ActorRef server = sys.actorOf(ServerActor.props(), "serverActor");

        // 4. Create the chosen Client actor, passing the server's ActorRef to its Props factory
        //    (Props.create(ClientClass.class, server)) so the client stores the server reference in a final field
        //    (Lecture 1 Slide 19: ActorRefs can be passed as constructor arguments or inside messages).
        final ActorRef client = useAsk
                ? sys.actorOf(ClientAskActor.props(server), "clientActor")
                : sys.actorOf(ClientActor.props(server), "clientActor");
        System.out.println("Using " + (useAsk ? "ClientAskActor (Ask pattern)" : "ClientActor (become + stash)"));

        // =========================================================================================
        // PHASE 2 & 3: Asynchronous command submission (main -> client) and actor execution flow
        // =========================================================================================
        // All 6 commands are sent asynchronously at once from the same sender (the main thread).
        // By Akka's default FIFO mailbox policy (Lecture 1 Slide 23: "FIFO order from a single sender"),
        // they are enqueued in clientActor's mailbox in exact submission order:
        //   [1] PutMsg("Alice", "alice@polimi.it")
        //   [2] PutMsg("Bob", "bob@polimi.it")
        //   [3] GetMsg("Alice")
        //   [4] GetMsg("Diana")
        //   [5] PutMsg("Bob", "bob.new@polimi.it")
        //   [6] GetMsg("Bob")
        // ActorRef.noSender() is used because the main thread is outside the ActorSystem and expects no reply;
        // clientActor will forward each request to serverActor passing self() as the sender reference.

        // [Messages 1 & 2] Insertions (fire-and-forget):
        // - ClientActor (in idle() behavior) matches PutMsg.class -> onPut(), prints the log, and forwards
        //   each message via server.tell(msg, self()). Because PutMsg expects no reply, ClientActor stays in idle().
        // - ServerActor matches PutMsg.class -> onPut() and stores "Alice" -> "alice@polimi.it" and
        //   "Bob" -> "bob@polimi.it" in its private HashMap.
        client.tell(new PutMsg("Alice", "alice@polimi.it"), ActorRef.noSender());
        client.tell(new PutMsg("Bob", "bob@polimi.it"), ActorRef.noSender());

        // [Message 3] Lookup for an existing contact ("Alice"):
        // - Case A (ClientActor - become + stash):
        //   1. In idle(), matches GetMsg.class -> onGet(), forwards GetMsg("Alice") via server.tell(msg, self()),
        //      and switches behavior via getContext().become(waitingForReply()).
        //   2. While waiting for ServerActor's ReplyMsg, any subsequent messages dequeued from clientActor's
        //      mailbox ([4] GetMsg("Diana"), [5] PutMsg("Bob", ...), [6] GetMsg("Bob")) do not match ReplyMsg.class
        //      and are saved in the actor's internal stash by .matchAny(msg -> stash()).
        //   3. ServerActor.onGet() finds "alice@polimi.it" in its HashMap, builds ReplyMsg.success("Alice", "alice@polimi.it"),
        //      and sends it back to the client via sender().tell(reply, self()) (Lecture 1 Slide 26).
        //   4. ClientActor (in waitingForReply()) matches ReplyMsg.class -> onReply(), prints
        //      "[Client] REPLY: Alice -> alice@polimi.it", calls unstashAll() to prepend the stashed messages
        //      back to the front of its mailbox in FIFO order (Lecture 2 Slide 9), and calls getContext().become(idle()).
        // - Case B (ClientAskActor - Ask pattern):
        //   1. onGet() calls Patterns.ask(server, msg, TIMEOUT_MS), which creates a temporary sender actor and
        //      returns a Future<Object>, then blocks on Await.result(future, timeout) (Lecture 2 Slide 11).
        //   2. ServerActor replies via sender().tell(reply, self()), completing the Future; onGet() unblocks,
        //      prints the reply, and returns so the next message in the mailbox can be processed.
        client.tell(new GetMsg("Alice"), ActorRef.noSender());

        // [Message 4] Lookup for a non-existent contact ("Diana"):
        // - Dequeued in idle() only after "Alice"'s ReplyMsg has been received and printed.
        // - ClientActor forwards GetMsg("Diana") via server.tell(msg, self()) and enters waitingForReply()
        //   (stashing [5] PutMsg("Bob", ...) and [6] GetMsg("Bob")).
        // - ServerActor.onGet() sees contacts.get("Diana") == null, builds ReplyMsg.notFound("Diana")
        //   (found = false, statusMessage = "Error: Contact 'Diana' not found in contact list"), and replies
        //   via sender().tell(reply, self()).
        // - ClientActor.onReply() prints the error message, calls unstashAll(), and reverts to idle().
        client.tell(new GetMsg("Diana"), ActorRef.noSender());

        // [Messages 5 & 6] Overwrite existing contact ("Bob") and query "Bob" again:
        // - Back in idle(), ClientActor dequeues [5] PutMsg("Bob", "bob.new@polimi.it"), forwards it via
        //   server.tell(msg, self()), and stays in idle().
        // - Next, it dequeues [6] GetMsg("Bob"), forwards it via server.tell(msg, self()), and enters waitingForReply().
        // - Because clientActor sends PutMsg("Bob", "bob.new@polimi.it") before GetMsg("Bob") to the same receiver
        //   (serverActor), FIFO ordering between the sender-receiver pair guarantees that ServerActor overwrites
        //   "Bob"'s email with "bob.new@polimi.it" before reading it for GetMsg("Bob").
        // - ServerActor replies with ReplyMsg.success("Bob", "bob.new@polimi.it"), and ClientActor prints
        //   "[Client] REPLY: Bob -> bob.new@polimi.it", calls unstashAll(), and returns to idle().
        client.tell(new PutMsg("Bob", "bob.new@polimi.it"), ActorRef.noSender());
        client.tell(new GetMsg("Bob"), ActorRef.noSender());

        // =========================================================================================
        // PHASE 5: Wait for completion and terminate the ActorSystem
        // =========================================================================================

        // Block the main thread until ENTER is pressed so the asynchronous message exchange finishes
        // before the ActorSystem is shut down.
        System.out.println("Press ENTER to terminate...");
        System.in.read();

        // Stop clientActor and serverActor and release the ActorSystem dispatcher threads
        sys.terminate();
    }
}
