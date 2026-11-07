package com.lab.evaluation22;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.List;

public class ConfigurationPublishMessage {
    private final ActorRef dispatcher;

    public ConfigurationPublishMessage(ActorRef dispatcher) {
        this.dispatcher = dispatcher;
    }

    public ActorRef getDispatcher() {
        return dispatcher;
    }
}
