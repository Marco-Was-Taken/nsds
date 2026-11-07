package com.polimi.nsds.akka.ex2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

/**
 * Test for Exercise 2: sequence decrement, decrement, increment, decrement, increment, increment.
 * <p>
 * All messages are sent by the same sender (the main thread), so they are enqueued in FIFO
 * order (slide "Mailbox semantics": FIFO order from a single sender). No sleeps are needed:
 * the outcome does not depend on timing.
 * </p>
 * <p>
 * <b>Step-by-step trace</b> (D = decrement, I = increment, mailbox initially D1 D2 I1 D3 I2 I3):
 * <ol>
 *   <li>D1: counter = 0, stash() &rarr; stash [D1]</li>
 *   <li>D2: counter = 0, stash() &rarr; stash [D1, D2]</li>
 *   <li>I1: counter = <b>1</b>; unstashAll() &rarr; mailbox D1 D2 D3 I2 I3</li>
 *   <li>D1: counter = <b>0</b></li>
 *   <li>D2: counter = 0, stash() &rarr; stash [D2]</li>
 *   <li>D3: counter = 0, stash() &rarr; stash [D2, D3]</li>
 *   <li>I2: counter = <b>1</b>; unstashAll() &rarr; mailbox D2 D3 I3</li>
 *   <li>D2: counter = <b>0</b></li>
 *   <li>D3: counter = 0, stash() &rarr; stash [D3]</li>
 *   <li>I3: counter = <b>1</b>; unstashAll() &rarr; mailbox D3</li>
 *   <li>D3: counter = <b>0</b></li>
 * </ol>
 * The printed values therefore evolve as 1, 0, 1, 0, 1, 0 and the counter never becomes negative.
 * (The same result is obtained if the messages arrive with delays, because stashed messages
 * keep their relative order.)
 * </p>
 */
public class CounterEx2 {

    /**
     * Sends the test sequence, waits for ENTER and terminates.
     *
     * @param args command-line arguments (not used)
     * @throws IOException if reading from standard input fails
     */
    public static void main(String[] args) throws IOException {
        // Create the actor system and the counter actor
        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef counter = sys.actorOf(CounterActor.props(), "counter");

        // Test sequence required by the exercise
        counter.tell(new DecrementMsg(), ActorRef.noSender());
        counter.tell(new DecrementMsg(), ActorRef.noSender());
        counter.tell(new IncrementMsg(), ActorRef.noSender());
        counter.tell(new DecrementMsg(), ActorRef.noSender());
        counter.tell(new IncrementMsg(), ActorRef.noSender());
        counter.tell(new IncrementMsg(), ActorRef.noSender());

        // Wait for all messages to be processed
        System.out.println("Press ENTER to terminate...");
        System.in.read();

        // Terminate the actor system
        sys.terminate();
    }
}
