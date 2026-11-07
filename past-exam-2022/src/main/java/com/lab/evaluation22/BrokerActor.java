package com.lab.evaluation22;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BrokerActor extends AbstractActorWithStash {
    private static Integer WORKERS_NUMBER = 2;
    private List<ActorRef> workers = new ArrayList<>(WORKERS_NUMBER);

    private SupervisorStrategy strategy = new OneForOneStrategy(
            1,
            Duration.ofSeconds(1),
            DeciderBuilder.match(Exception.class, this::onExceptionHandling).build()
    );

    private SupervisorStrategy.Directive onExceptionHandling(Exception e){
        System.out.println("Exception: " + e.getMessage());
        return SupervisorStrategy.resume();
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

    public BrokerActor() {
        for(int i = 0; i < WORKERS_NUMBER; i++){
            workers.add(getContext().actorOf(WorkerActor.props(), "worker" + i));
        }
    }

    @Override
    public Receive createReceive() {
        return onDefaultMode();
    }

    public Receive onDefaultMode(){
        return receiveBuilder()
                .match(BatchMsg.class, this::onBatchMessage)
                .match(PublishMsg.class, this::onDefaultMessage)
                .match(SubscribeMsg.class, this::onDefaultMessage)
                .build();
    }

    public void onDefaultMessage(Object o){
        stash();
    }

    public void onBatchMessage(BatchMsg batchMsg){
        if(batchMsg.isOn()){
            getContext().become(onBatchMode());
        }
        else {
            unstashAll();
            getContext().become(onNormalMode());
        }
    }

    public Receive onBatchMode(){
        return receiveBuilder()
                .match(SubscribeMsg.class, this::onSubscribeMessage)
                .match(BatchMsg.class, this::onBatchMessage)
                .match(PublishMsg.class, this::onPublishBatchMessage)
                .build();
    }

    public Receive onNormalMode(){
        return receiveBuilder()
                .match(PublishMsg.class, this::onPublishMessage)
                .match(SubscribeMsg.class, this::onSubscribeMessage)
                .match(BatchMsg.class, this::onBatchMessage)
                .build();
    }

    public void onPublishMessage(PublishMsg msg){
        for(ActorRef worker : workers){
            worker.tell(msg, getSelf());
        }
    }

    public void onSubscribeMessage(SubscribeMsg msg){
        Integer workerIndex = msg.getKey() % WORKERS_NUMBER;
        ActorRef worker = workers.get(workerIndex);
        worker.tell(msg, getSelf());
    }

    public void onPublishBatchMessage(PublishMsg msg){
        stash();
    }


    public static Props props(){
        return Props.create(BrokerActor.class);
    }

}
