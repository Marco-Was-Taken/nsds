import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;

public class CounterCoordinatorActor extends AbstractActorWithStash {
	private final Props worker0Props;
	private final Props worker1Props;
	private final ActorRef[] workers = new ActorRef[2];

	private int workerReportCounter;
	private int counterSum;
	private int valueSum;
	private ActorRef clientActor;
	
	public CounterCoordinatorActor(Props worker0Props, Props worker1Props) {
		this.worker0Props = worker0Props;
		this.worker1Props = worker1Props;
	}

	public static Props props() {
		return props(CounterWorkerActor.props(0), CounterWorkerActor.props(1));
	}

	public static Props props(Props worker0Props, Props worker1Props) {
		return Props.create(CounterCoordinatorActor.class,
				() -> new CounterCoordinatorActor(worker0Props, worker1Props));
	}

	@Override
	public void preStart() {
		// Create the two workers used by this coordinator
		workers[0] = getContext().actorOf(worker0Props, "worker0");
		workers[1] = getContext().actorOf(worker1Props, "worker1");
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RecordMsg.class, this::onRecordMsg)
				.match(ReportMsg.class, this::onReportMsg)
				.build();
	}

	public void onRecordMsg(RecordMsg msg) {
		if(msg.getKey() % 2 == 0) {
			workers[0].tell(msg, self());
		} else {
			workers[1].tell(msg, self());
		}
	}

	public void onReportMsg(ReportMsg msg) {
		getContext().become(onReportMode());
		workers[0].tell(msg, self());
		workers[1].tell(msg, self());

		clientActor = getSender();

		workerReportCounter = 0;
		counterSum = 0;
		valueSum = 0;
	}

	public Receive onReportMode(){
		return receiveBuilder()
				.match(ReportMsg.class, this::onReportMsgDuringReport)
				.match(RecordMsg.class, this::onRecordMsgDuringReport)
				.match(ReportStateCounterMessage.class, this::onReportStateCounterMessage)
				.build();
	}

	public void onReportMsgDuringReport(ReportMsg msg) {
		stash();
	}
	public void onRecordMsgDuringReport(RecordMsg msg) {
		stash();
	}

	public void onReportStateCounterMessage(ReportStateCounterMessage msg) {
		workerReportCounter++;

		counterSum += msg.getCounter();
		valueSum += msg.getSum();

		if(workerReportCounter == 2) {
			clientActor.tell(new ReportTotalMessage(counterSum, valueSum), self());
			getContext().become(createReceive());
			unstashAll();
		}
	}

}
