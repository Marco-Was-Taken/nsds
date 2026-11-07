package com.lab.evaluation23;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class DispatcherActor extends AbstractActorWithStash {

	private final static int NO_PROCESSORS = 2;

	public DispatcherActor() {
	}

	private SupervisorStrategy strategy = new OneForOneStrategy(
			1,
			Duration.ofMillis(500),
			DeciderBuilder
					.match(Exception.class, e -> SupervisorStrategy.resume())
			.build()
	);

	@Override
	public SupervisorStrategy supervisorStrategy(){
		return strategy;
	}

	private HashMap<ActorRef, Integer> load = new HashMap<>();
	private HashMap<ActorRef, ActorRef> temperatureToData = new HashMap<>();

	private List<ActorRef> sensors = new ArrayList<>();
	private Integer currentSensor;

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(DispatchLogicMsg.class,this::onSetupMode)
				.build();
	}

	public void onSetupMode(DispatchLogicMsg msg) {
		for(int i = 0; i < NO_PROCESSORS; i++){
			getContext().actorOf(SensorProcessorActor.props(), "SensorActor" + i);
		}

		onChangeMode(msg);
	}

	public void onChangeMode(DispatchLogicMsg msg) {
		if(msg.getLogic() == DispatchLogicMsg.ROUND_ROBIN){
			currentSensor = 0;
			for(ActorRef sensor : getContext().getChildren()){
				sensors.add(sensor);
			}
			getContext().become(onRoundRobinMode());
		}
		else if(msg.getLogic() == DispatchLogicMsg.LOAD_BALANCER){
			for(ActorRef sensor : getContext().getChildren()){
				temperatureToData = new HashMap<>();
				load.put(sensor, 0);
			}
			getContext().become(onLoadBalancerMode());
		}
	}

	public Receive onRoundRobinMode(){
		return receiveBuilder()
				.match(DispatchLogicMsg.class,this::onChangeMode)
				.match(TemperatureMsg.class, this::dispatchDataRoundRobin)
				.build();
	}

	public Receive onLoadBalancerMode(){
		return receiveBuilder()
				.match(DispatchLogicMsg.class,this::onChangeMode)
				.match(TemperatureMsg.class, this::dispatchDataLoadBalancer)
				.build();
	}

	private void dispatchDataLoadBalancer(TemperatureMsg msg) {
		ActorRef sensor = temperatureToData.get(msg.getSender());
		if(sensor != null){
			sensor.tell(msg, self());
		}
		else{
			ActorRef sensorActor = nextSensorForLoad();
			load.put(sensorActor,load.get(nextSensorForLoad()) + 1);
			temperatureToData.put(msg.getSender(),sensorActor);
			sensorActor.tell(msg, self());

		}
	}

	private void dispatchDataRoundRobin(TemperatureMsg msg) {
		sensors.get(currentSensor).tell(msg, self());
		currentSensor= (currentSensor+1)%NO_PROCESSORS;
	}

	private ActorRef nextSensorForLoad(){
		int min = 0;
		ActorRef sensor = null;
		for(ActorRef s : load.keySet()){
			if(load.get(s)<=min){
				min = load.get(s);
			}
			sensor = s;
		}
		return sensor;
	}

	static Props props() {
		return Props.create(DispatcherActor.class);
	}
}
