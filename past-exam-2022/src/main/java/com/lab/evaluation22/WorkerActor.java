package com.lab.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;

public class WorkerActor extends AbstractActor {
    private HashMap<String, ActorRef> topicToSubscriber;

    WorkerActor(){
        this.topicToSubscriber = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubscribeMsg.class, this::onSubscribeMessage)
                .match(PublishMsg.class, this::onPublishMessage)
                .build();
    }

    private void onSubscribeMessage(SubscribeMsg subscribe){
        this.topicToSubscriber.put(subscribe.getTopic(), subscribe.getSender());
    }

    private void onPublishMessage(PublishMsg publish) throws Exception{
        ActorRef subscriber = this.topicToSubscriber.get(publish.getTopic());

        if(subscriber != null)
            subscriber.tell(new NotifyMsg(publish.getValue()), getSelf());
        else throw new Exception("No subscriber for topic: " + publish.getTopic() + " from " + getContext().getSelf().toString());
    }

    public static Props props(){
        return Props.create(WorkerActor.class);
    }
}
