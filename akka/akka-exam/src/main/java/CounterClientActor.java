import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import static akka.pattern.Patterns.ask;

public class CounterClientActor extends AbstractActor {

	private static final Duration INTERLEAVE = Duration.ofMillis(100);
	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(5000, TimeUnit.MILLISECONDS);

	private final ActorRef coordinator;
	private List<Msg> script;
	private int id;

	private final List<Msg> script0 = Arrays.asList(new RecordMsg(0, 10), new RecordMsg(1, 7), new ReportMsg(false),
			new RecordMsg(1, 3), new ReportMsg(true), new ReportMsg(false), new RecordMsg(3, 4), new RecordMsg(0, 0),
			new ReportMsg(false));

	private final List<Msg> script1 = Arrays.asList(new RecordMsg(0, 4), new ReportMsg(false), new RecordMsg(2, 3),
			new RecordMsg(2, 5), new ReportMsg(false), new RecordMsg(3, 4), new RecordMsg(0, 0), new ReportMsg(false));


	private int position;
	private boolean started;

	public CounterClientActor(ActorRef coordinator, int script) {
		this.coordinator = coordinator;
		this.position = 0;
		this.id = script;
		if (script == 0) {
			this.script = script0;
		} else {
			this.script = script1;
		}
	}

	public static Props props(ActorRef coordinator, int script) {
		return Props.create(CounterClientActor.class, () -> new CounterClientActor(coordinator, script));
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(TestInterleaveMsg.class, this::sendNext).match(StartMsg.class, msg -> {
			if (!started) {
				started = true;
				sendNext(null);
			}
		}).build();
	}

	private void sendNext(TestInterleaveMsg msg) {

		if (position == script.size()) {
			System.out.println("CLIENT" + id + " -> test completed!");
			return;
		}

		Msg request = script.get(position++);
		System.out.println("CLIENT" + id + " -> " + request);

		if(request instanceof RecordMsg) {
			RecordMsg recordMsg = (RecordMsg) request;
			coordinator.tell(recordMsg, self());
		}
		if(request instanceof ReportMsg) {
			ReportMsg reportMsg = (ReportMsg) request;
			Future<Object> future = Patterns.ask(coordinator, reportMsg, 10000);

			try{
				ReportTotalMessage reportTotalMessage = (ReportTotalMessage) future.result(timeout, null);
				System.out.println("The total counter is " + reportTotalMessage.getCounterSum() + " and the total sum is " + reportTotalMessage.getValueSum());
			} catch (Exception e) {
				System.out.println(getContext().getSelf().toString() + "Error occurred while processing report message: " + e.getMessage());
			}
		}

		getContext().system().scheduler().scheduleOnce(INTERLEAVE, self(), new TestInterleaveMsg(),
				getContext().dispatcher(), self());

	}
}
