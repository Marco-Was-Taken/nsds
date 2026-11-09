import akka.actor.ActorRef;

public class ReportEndMsg extends Msg {

    private final ReportMsg msg;
    private final ActorRef actorRef;

    public ReportEndMsg(ReportMsg msg, ActorRef actorRef) {
        this.msg = msg;
        this.actorRef = actorRef;
    }

    public ReportMsg getMsg() {
        return msg;
    }

    public ActorRef getActorRef() {
        return actorRef;
    }
}
