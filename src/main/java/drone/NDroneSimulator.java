package drone;

import eventMediatorLocator.EventMediatorLocator;
import remotes.DroneRemoteIF;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by mohannad on 07/01/16.
 */
public class NDroneSimulator {

public static void main(String args []){
    int n =  1000;
    for(int i=0 ; i< n ; i++){
        Thread t = new Thread(new NDroneSimulator.DroneTask(i));
        t.start();
    }
}

    static class DroneTask implements Runnable{
        int id;
        public DroneTask(int id){
            this.id = id;
        }
        @Override
        public void run() {
            Random genetator = new Random();
            try {
                //ArrayList<String> drones = EventMediatorLocator.mediator().listDrones();
                //int len =EventMediatorLocator.mediator().size();
                //if(drones!=null) len=drones.size();
                DroneRemoteIF drone = new Drone("drone@"+id);
                System.out.println(drone.getName()+" is up and running");

            } catch (Exception e) {
                System.err.println("Client exception: " + e.toString());
                e.printStackTrace();
            }
        }

    }
}
