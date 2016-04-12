package mapreduce.tasktracker;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.protobuf.InvalidProtocolBufferException;

import mapreduce.MapReduce;
import mapreduce.MapReduce.HeartBeatResponse;
import mapreduce.MapReduce.MapTaskInfo;
import mapreduce.MapReduce.MapTaskStatus;
import mapreduce.MapReduce.ReducerTaskInfo;
import mapreduce.jobtracker.IJobTracker;

public class TaskTracker {
	
	private static String jobTrackerIP;
	private IJobTracker jobTracker=null;
	private static final String CONFIG = "./config/config.ini";
	private static final String JOB_TRACKER="jobtracker";
	private int tid;
	private MapperThreadPool mapperThreadPool = null;
	//TODO:similarly reducer threadpool has to be implementeed

	public TaskTracker(int id) {
		System.out.println("Info: Connecting to Jobtracker:"+jobTrackerIP+"  From Tasktracker");
		this.tid=id;
		connectToJobTracker();
		mapperThreadPool = new MapperThreadPool();
		
	}
	
	static{
		Scanner sc = null;
		String data[];
		try {
			sc = new Scanner(new File(CONFIG));
			while (sc.hasNext()) {
				data = sc.nextLine().split("=");
				if (JOB_TRACKER.equals(data[0])) {
					jobTrackerIP = data[1];
					System.out.println("INFO:" +data);
				} 
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (sc != null)
				sc.close();
		}
	}
	
	
	private void connectToJobTracker() {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			Registry registry = LocateRegistry.getRegistry(jobTrackerIP);
			jobTracker = (IJobTracker) registry.lookup("JobTracker");
		} catch ( RemoteException | NotBoundException e) {
			System.out.println("ERROR: Error in connecting to JobTracker......");
			e.printStackTrace();
		}
	}
	
	public MapReduce.HeartBeatResponse sendHeartBeat(){
		//System.out.println("INFO: Sending HeartBeat");
		MapReduce.HeartBeatRequest.Builder heartBeatBuilder = MapReduce.HeartBeatRequest.newBuilder();
		MapReduce.HeartBeatResponse heartBeatResponse = null;
		heartBeatBuilder.setTaskTrackerId(tid);
		heartBeatBuilder.setNumMapSlotsFree(MapperThreadPool.availableCount);
		//System.out.println("INFO: Attaching tib and freeslots to heartbeat successfull");
		//TODO: Need to add info of reducer Thread pool
		for ( MapTaskStatus mapTaskStatus : MapperThreadPool.activeThreadTask) {
			heartBeatBuilder.addMapStatus(mapTaskStatus);
		}
		
		for (MapTaskStatus mapTaskStatus : MapperThreadPool.completedThreadTask) {
			heartBeatBuilder.addMapStatus(mapTaskStatus);
		}
		
		MapperThreadPool.flushCompletedList();
		try {
		
		//System.out.println("INFO: Appending Task status to heartbeat was successfull");
		byte[] heartBeatResponseByte = jobTracker.heartBeat(heartBeatBuilder.build().toByteArray());
		
			//System.out.println("INFO: Recieved heartbeat response from Job tracker");
			heartBeatResponse= MapReduce.HeartBeatResponse.parseFrom(heartBeatResponseByte);
		} catch (InvalidProtocolBufferException | RemoteException e) {
			e.printStackTrace();
		}
		return heartBeatResponse;
		
	}
	
	public void assignTaskToThread(HeartBeatResponse heartBeatResponse){
		//System.out.println("INFO: Assigning JT requested tasks to thread");
		//System.out.println("INFO: Checking MapTasks");
		for (MapTaskInfo mapTaskInfo : heartBeatResponse.getMapTasksList()) {
			mapperThreadPool.addNewMapTask(mapTaskInfo);
		}
		
		/*for (ReducerTaskInfo reduceTaskInfo : heartBeatResponse.getReduceTasksList()) {
			mapperThreadPool.addNewReduceTask(reduceTaskInfo);
		}
		*/
		
		
		//System.out.println("INFO: Checking ReduceTasks");
	}
	
	public static void main(String[] args) {
		if(args.length!=1){
			System.out.println("ERROR:Invalid numer of input");
			return;
		}
		int id=Integer.parseInt(args[0]);
		TaskTracker taskTracker = new TaskTracker(id);
		
		while(true){
			try {
				Thread.sleep(2000);
				HeartBeatResponse heartBeatResponse=taskTracker.sendHeartBeat();
				taskTracker.assignTaskToThread(heartBeatResponse);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	

}
