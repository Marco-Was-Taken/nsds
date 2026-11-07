package com.lab.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class SubscriberActor extends AbstractActor {
    private ActorRef dispatcher;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ConfigurationSubscribeMessage.class, this::onConfigurationSubscribeMessage)
                .match(NotifyMsg.class, this::onNotifyMessage)
                .match(SubscribeMsg.class, this::onSubscriberMessage)
                .build();
    }

    private void onConfigurationSubscribeMessage(ConfigurationSubscribeMessage message){
        dispatcher = message.getDispatcher();
    }

    private void onSubscriberMessage(SubscribeMsg message){
        dispatcher.tell(message, getSelf());
    }

    private void onNotifyMessage(NotifyMsg message){
        System.out.println("Received notification: " + message.getValue());
    }

    public static Props props(){
        return Props.create(SubscriberActor.class);
    }
}
