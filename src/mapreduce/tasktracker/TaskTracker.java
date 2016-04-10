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
import mapreduce.MapReduce.MapTaskStatus;
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
	
	public void removeCompletedTask(List<String> completedList){
		for (String key : completedList) {
			mapperThreadPool.activeThreadTask.remove(key);
			mapperThreadPool.availableCount++;
			mapperThreadPool.occupiedCount--;
		}
		System.out.println("INFO: completed Tasks removed from the queue");
	}
	
	public MapReduce.HeartBeatResponse sendHeartBeat(){
		System.out.println("INFO: Sending HeartBeat");
		MapReduce.HeartBeatRequest.Builder heartBeatBuilder = MapReduce.HeartBeatRequest.newBuilder();
		MapReduce.HeartBeatResponse heartBeatResponse = null;
		heartBeatBuilder.setTaskTrackerId(tid);
		heartBeatBuilder.setNumMapSlotsFree(mapperThreadPool.availableCount);
		System.out.println("INFO: Attaching tib and freeslots to heartbeat successfull");
		//TODO: Need to add info of reducer Thread pool
		List<String> completedList = new ArrayList<>();
		for ( Map.Entry<String,MapTaskStatus> mapTaskStatus : mapperThreadPool.activeThreadTask.entrySet()) {
			heartBeatBuilder.addMapStatus(mapTaskStatus.getValue());
			if(mapTaskStatus.getValue().getTaskCompleted()){
				completedList.add(mapTaskStatus.getKey());
				System.out.println("INFO: Task "+mapTaskStatus.getKey()+" got completed");
			}
			
		}
		
		removeCompletedTask(completedList);
		System.out.println("INFO: Appending Task status to heartbeat was successfull");
		byte[] heartBeatResponseByte = jobTracker.heartBeat(heartBeatBuilder.build().toByteArray());
		try {
			System.out.println("INFO: Recieved heartbeat response from Job tracker");
			heartBeatResponse= MapReduce.HeartBeatResponse.parseFrom(heartBeatResponseByte);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return heartBeatResponse;
		
	}
	
	public void assignTaskToThread(HeartBeatResponse heartBeatResponse){
		System.out.println("INFO: Assigning JT requested tasks to thread");
		System.out.println("INFO: Checking MapTasks");
		
		System.out.println("INFO: Checking ReduceTasks");
	}
	
	public static void main(String[] args) {
		if(args.length!=1){
			System.out.println("ERROR:Invalid numer of input");
			return;
		}
		int id=Integer.parseInt(args[0]);
		TaskTracker taskTracker = new TaskTracker(id);
	}
	
	

}
