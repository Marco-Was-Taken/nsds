import akka.actor.AbstractActor;
import akka.actor.Props;

import java.time.Duration;

public class CounterWorkerActor extends AbstractActor{
	private final int workerId;

	private int counterSum;
	private int valueSum;

	public CounterWorkerActor(int workerId) {
		this.workerId = workerId;
	}

	public static Props props(int workerId) {
		return Props.create(CounterWorkerActor.class, () -> new CounterWorkerActor(workerId));
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RecordMsg.class, this::onRecordMsg)
				.match(ReportMsg.class, this::onReportMsg)
				.match(ReportEndMsg.class, this::onRecordEndMessage)
				.build();
	}

	public void onRecordMsg(RecordMsg recordMsg) {
		counterSum ++;
		valueSum += recordMsg.getValue();
	}

	public void onReportMsg(ReportMsg reportMsg) {
		getContext().getSystem().scheduler().scheduleOnce(
				Duration.ofMillis(3000),
				getSelf(),
				new ReportEndMsg(reportMsg, getSender()),
				getContext().dispatcher(),
				getSelf()
		);
	}

	private  void onRecordEndMessage(ReportEndMsg msg) {
		;msg.getActorRef().tell(new ReportStateCounterMessage(counterSum, valueSum), getSelf());

		if(msg.getMsg().isReset()){
			counterSum = 0;
			valueSum = 0;
		}
	}

}
