package tracer;

import drone.Drone;
import endPoints.AdressEndPoints;
import eventMediatorLocator.EventMediatorLocator;
import path.Path;
import pathFinder.GooglePathFinder;
import pathFinder.PathPlannerStrategy;
import remotes.DroneRemoteIF;
import remotes.TracerIF;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by mohannad on 07/01/16.
 */
public class NTracerSimulator {
    public static void main(String args []){
        int n = 1000;
        for(int i=0 ; i< n ; i++){
            Thread t = new Thread(new NTracerSimulator.TracerTask());
            t.start();
        }
    }

    static class TracerTask implements Runnable {

        public TracerTask() {
        }

        @Override
        public void run() {
            Random genetator = new Random();
            try {
                String drone = EventMediatorLocator.mediator().allocateDrone();
                if (drone != null) {
                    String dest[] = {"Dijon", "Lille", "Rennes", "Tours", "Caen", "Nantes", "Angers"};
                    Random random = new Random();
                    int idx1 = random.nextInt(7);
                    String destination = dest[idx1];
                    int idx2 = random.nextInt(7);
                    if (idx2 == idx1) {
                        idx2 = (idx2 + 1) % dest.length;
                    }
                    String start = dest[idx2];
                    PathPlannerStrategy pathPlanner = new GooglePathFinder();
                    System.out.println(start + ":" + destination);
                    Path p = pathPlanner.findPath(new AdressEndPoints(start, destination));
                    EventMediatorLocator.mediator().loadPath(drone, p.getPathPoints());
                    TracerIF tracer = new Tracer(drone, "tracer@" + drone.substring(drone.lastIndexOf("@")+1));
                    Thread.sleep(genetator.nextInt(1000));
                    EventMediatorLocator.mediator().startDelivary(drone);


                }
            } catch (RemoteException e) {
                e.printStackTrace();
            } catch (NotBoundException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}