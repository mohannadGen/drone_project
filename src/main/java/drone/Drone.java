package drone;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import drone.convertor.DummyPathConverter;
import eventMediatorLocator.EventMediatorLocator;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import path.PathPoint;
import path.Path;
import pathToNavCommands.Command;
import pathToNavCommands.PathToComandStrategy;
import remotes.DroneRemoteIF;
import remotes.MediatorIF;
import remotes.Notifiable;

public class Drone implements DroneRemoteIF,Moveable {
	
	ArrayList<Command> commands;
	//MediatorIF mediator;
    Producer<String,String> producer; //For the moment, we can simply produce string messages
	PathToComandStrategy convertor;
	String name; //The name will be used as the name of the topic

	public Drone(String name, String brokers) {
		this.name =name;
		this.convertor = new DummyPathConverter(this);
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //The partitioning
        //props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);
        /*this.mediator = EventMediatorLocator.mediator();
        this.mediator.registerDrone(this);*/
	}

	@Override
	public void loadPath(ArrayList<PathPoint> path) throws RemoteException {
		this.commands = this.convertor.convertPath(path);
		System.out.println("Path has been loaded successfully ......");
	}


	@Override
	public void goTo(PathPoint point){
        producer.send(new KeyedMessage<String,String>(point.toString(),name));
        //this.mediator.notifyLocation(name,point);
    }
	@Override
	public void go() throws RemoteException {
        System.out.println("Drone is up and heading the destination....");
        System.out.print("[");
        //Iterator<Command> ir = commands.iterator();
        for(int i = commands.size()-1 ; i >=0 ; i--){
            Command c = commands.get(i);
            c.execute();
            if(i%50 == 0)  System.out.print("=");
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        System.out.println(">]");
        this.mediator.notifyArrivale(this.name);
        System.out.println("Drone has reached its destination .....");
	}

    @Override
    public String getName() throws RemoteException {
        return this.name;
    }


    public static void main(String args[]) throws RemoteException, NotBoundException, MalformedURLException {
        ArrayList<String> drones = EventMediatorLocator.mediator().listDrones();
        int len =0;
        if(drones!=null) len=drones.size();
        DroneRemoteIF drone = new Drone("drone"+len);
        System.out.println("drone"+len+" is up and running");
    }
}
