package com.lab.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class PublisherActor extends AbstractActor {

    private ActorRef dispatcher;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ConfigurationPublishMessage.class, this::onConfigurationPublishMessage)
                .match(PublishMsg.class, this::onPublishMessage)
                .build();
    }

    private void onConfigurationPublishMessage(ConfigurationPublishMessage message) {
        dispatcher = message.getDispatcher();
    }

    private void onPublishMessage(PublishMsg message) {
        dispatcher.tell(message, getSelf());
    }

    public static Props props(){
        return Props.create(PublisherActor.class);
    }
}
