package com.polimi.nsds.akka.ex5;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;

/**
 * Supervisor actor for Exercise 5 that creates and supervises {@link ServerActor}
 * (Lecture 3, Slides 9–13).
 * <p>
 * <b>How Supervision Works in Akka:</b>
 * <ul>
 *   <li>An actor is the supervisor of all child actors it creates in its own context via
 *       {@code getContext().actorOf(props)} (Lecture 3, Slide 9).</li>
 *   <li>By overriding {@link #supervisorStrategy()}, this supervisor defines how to handle
 *       exceptions thrown by its child {@link ServerActor}:
 *       <ul>
 *         <li><b>{@code resume} mode ({@code useRestart == false}):</b> returns
 *             {@code SupervisorStrategy.resume()}, keeping the same {@link ServerActor} instance
 *             and preserving its internal {@code contacts} map;</li>
 *         <li><b>{@code restart} mode ({@code useRestart == true}):</b> returns
 *             {@code SupervisorStrategy.restart()}, replacing the crashed {@link ServerActor}
 *             instance with a fresh one (which resets the {@code contacts} map to empty) while
 *             preserving the child's {@link ActorRef} and mailbox (Lecture 3, Slide 7).</li>
 *       </ul>
 *   </li>
 * </ul>
 * </p>
 */
public class SupervisorActor extends AbstractActor {

    // Maximum number of retries allowed within the 1-minute window before stopping the child
    private static final int MAX_RETRIES = 10;

    // Strategy 1: RESUME on Exception -> preserves the child's internal state (contact list)
    private static final SupervisorStrategy RESUME_STRATEGY =
            new OneForOneStrategy(
                    MAX_RETRIES,
                    Duration.ofMinutes(1),
                    DeciderBuilder.match(Exception.class, e -> {
                        System.out.println("[Supervisor] Caught '" + e.getMessage() + "' -> applying RESUME (state preserved)");
                        return SupervisorStrategy.resume();
                    }).build()
            );

    // Strategy 2: RESTART on Exception -> replaces the child instance and resets its contact list
    private static final SupervisorStrategy RESTART_STRATEGY =
            new OneForOneStrategy(
                    MAX_RETRIES,
                    Duration.ofMinutes(1),
                    DeciderBuilder.match(Exception.class, e -> {
                        System.out.println("[Supervisor] Caught '" + e.getMessage() + "' -> applying RESTART (state reset)");
                        return SupervisorStrategy.restart();
                    }).build()
            );

    // Selected strategy for this supervisor instance
    private final boolean useRestart;

    /**
     * Creates a supervisor configured with either the {@code restart} or {@code resume} strategy.
     *
     * @param useRestart {@code true} to use {@code SupervisorStrategy.restart()},
     *                   {@code false} to use {@code SupervisorStrategy.resume()}
     */
    public SupervisorActor(boolean useRestart) {
        this.useRestart = useRestart;
    }

    /**
     * Overrides the default supervision strategy with the selected {@link OneForOneStrategy}
     * (Lecture 3, Slide 11).
     *
     * @return {@link #RESTART_STRATEGY} if {@code useRestart} is {@code true},
     *         {@link #RESUME_STRATEGY} otherwise
     */
    @Override
    public SupervisorStrategy supervisorStrategy() {
        return useRestart ? RESTART_STRATEGY : RESUME_STRATEGY;
    }

    /**
     * Receives a {@link Props} object, spawns the child actor inside this supervisor's context
     * via {@code getContext().actorOf(props)}, and replies to {@code sender()} with the child's
     * {@link ActorRef} (Lecture 3, Slide 13).
     *
     * @return the {@link Receive} behavior of the supervisor
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Props.class, props -> {
                    // Create the child inside the supervisor's context so this actor supervises it
                    ActorRef child = getContext().actorOf(props, "serverActor");
                    // Reply to the caller (main's ask future) with the supervised child's ActorRef
                    sender().tell(child, self());
                })
                .build();
    }

    /**
     * Factory method returning the {@link Props} used to instantiate {@link SupervisorActor}.
     *
     * @param useRestart {@code true} for {@code restart()} strategy, {@code false} for {@code resume()} strategy
     * @return {@link Props} for {@link SupervisorActor}
     */
    public static Props props(boolean useRestart) {
        return Props.create(SupervisorActor.class, () -> new SupervisorActor(useRestart));
    }
}
