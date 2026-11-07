package com.lab.evaluation22;

import akka.actor.ActorRef;

public class ConfigurationSubscribeMessage {
    private final ActorRef dispatcher;

    public ConfigurationSubscribeMessage(ActorRef dispatcher){
        this.dispatcher = dispatcher;
    }

    public ActorRef getDispatcher(){
        return dispatcher;
    }
}
