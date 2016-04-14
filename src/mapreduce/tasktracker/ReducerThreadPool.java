package mapreduce.tasktracker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.ByteString;

import IDataNode.IDataNode;
import mapreduce.IReducer;
import mapreduce.MapReduce.DataNodeLocation;
import mapreduce.MapReduce.ReduceTaskStatus;
import mapreduce.MapReduce.ReducerTaskInfo;
import util.Connector;

public class ReducerThreadPool {
	private static final String CONFIG = "./config/config.ini";
	private static final String BLOCK = "blocksize";
	private static final String NAMENODE = "namenode";
	public static final String DOWNLOADED_FILE_PATH="./remoteData";
	private static int blockSize;
	
	
	public static int maxThreadCount = 5;
	public static int availableCount = maxThreadCount;
	public static Object lock = new Object();
	public static List<ReduceTaskStatus> activeThreadTask = null;
	public static List<ReduceTaskStatus> completedThreadTask = null;
	private ExecutorService reducerExecutor = null;
	
	
	static{
		Scanner sc = null;
		String data[];
		try {
			sc = new Scanner(new File(CONFIG));
			while (sc.hasNext()) {
				data = sc.nextLine().split("=");
				if (BLOCK.equals(data[0])) {
					blockSize = Integer.parseInt(data[1]);
					System.out.println("INFO: block size " + blockSize);
				} 
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (sc != null)
				sc.close();
		}
		
	}
	
	

	public ReducerThreadPool() {
		System.out.println("INFO: Reducer Thread Constructor Started");
		activeThreadTask = new ArrayList<>();
		completedThreadTask = new ArrayList<>();
		reducerExecutor = Executors.newFixedThreadPool(maxThreadCount);
		

	}

	public void addNewReducerTask(ReducerTaskInfo reducerTaskInfo) {
		Runnable worker = new ReducerWorker(reducerTaskInfo);
		reducerExecutor.execute(worker);
	}

	public static void flushCompletedList() {
		synchronized (lock) {
			completedThreadTask.clear();
		}
	}

}

class ReducerWorker implements Runnable {
	private static final String DELI = "/";
	private ReducerTaskInfo reducerTaskInfo;
	private Connector connector;

	public ReducerWorker(ReducerTaskInfo reducerTaskInfo) {
		this.reducerTaskInfo = reducerTaskInfo;
		connector = Connector.getConnector();
	}

	public void dumpReducerFileToHDFS(){
		String fileName = reducerTaskInfo.getOutputFile();
		System.out.println("INFO: Dumping file: "+fileName+"");
		connector.put(fileName);
	}

	public void executeReducerTask() {
		// Connect to data Node to get Block
		
		//for all MapOutput files
			//	get it from HDFS
			//	open the above file
			//	for each line call reducer.reduce()
			// if not null, Then put it to reduce_output file
		
		Scanner sc =  null;
		String outputFileName=reducerTaskInfo.getOutputFile();
		PrintWriter pr = null;
		boolean isempty=true;
		try {
			pr= new  PrintWriter(new File(outputFileName));
			Class cls = Class.forName(reducerTaskInfo.getReducerName());
			IReducer reducer = (IReducer) cls.newInstance();
		
		
			for (String mapFile : reducerTaskInfo.getMapOutputFilesList()) {
				System.out.println("trying to fetch "+mapFile + " from HDFS");
				connector.get(mapFile);
				System.out.println(mapFile + " downloaded");
				sc = new Scanner(new File(ReducerThreadPool.DOWNLOADED_FILE_PATH+DELI+mapFile));
				sc.nextLine();//Ignoring Header
				while(sc.hasNext()){
					String line=reducer.reduce(sc.nextLine());
					if(line!=null){
						pr.println(line);
						isempty=false;
					}
						
				}
				if(sc!=null)
					sc.close();
			}
			if(isempty){
				pr.println("*************NO Search Term Found in This Block*****************");
			}
		
		} catch (FileNotFoundException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}finally{
			if(pr!=null){
				pr.close();
			}
		}
		System.out.println("RESULT: "+outputFileName+" Created SuccessFully");
		connector.put(outputFileName);
		System.out.println("SUCCESS: "+outputFileName+ " Successfully uploaded to HDFS");
		
	}

	@Override
	public void run() {

		synchronized (ReducerThreadPool.lock) {
			System.out.println("INFO: Inside Critical Section");
			System.out.println("INFO: reducing available count of reducer to: "
					+ (ReducerThreadPool.availableCount - 1));
			ReducerThreadPool.availableCount--;
			// Build mapTaskStatusBuilder
			System.out.println("INFO: Building ReduceTaskStatus");
			ReduceTaskStatus.Builder reduceTaskStatusBuiler = ReduceTaskStatus.newBuilder();
			reduceTaskStatusBuiler.setJobId(reducerTaskInfo.getJobId());
			reduceTaskStatusBuiler.setTaskId(reducerTaskInfo.getTaskId());
			reduceTaskStatusBuiler.setTaskCompleted(false);

			// add this above to active map
			System.out.println("INFO: Added the reduceTaskStatus to ActiveThread");
			ReducerThreadPool.activeThreadTask.add(reduceTaskStatusBuiler.build());
			System.out.println("INFO: Exit from critical Section");
		}
		System.out.println("INFO: Execution Of Reduce Task Started");
		executeReducerTask();
		System.out.println("INFO: Execution Of Reduce Task Completed");

		synchronized (ReducerThreadPool.lock) {
			System.out.println("INFO: Entry to CS");
			System.out.println("INFO: Increasing available count of Reducer Thread to: "
					+ (ReducerThreadPool.availableCount + 1));
			ReducerThreadPool.availableCount++;
			// Flip completed flag in the map to true and move the entry set
			// from activeMap to completedMap
			int index = -1, requiredIndex = -1;
			ReduceTaskStatus redStatus = null;
			for (ReduceTaskStatus reduceStatus : ReducerThreadPool.activeThreadTask) {
				index++;
				if (reduceStatus.getJobId() == reducerTaskInfo.getJobId()
						&& reduceStatus.getTaskId() == reducerTaskInfo.getTaskId()) {
					System.out.println("INFO: Found the entry of completed task in ActiveTaskList");
					requiredIndex = index;
					redStatus = reduceStatus;
					break;
				}
			}
			ReduceTaskStatus.Builder reduceBuild = ReduceTaskStatus.newBuilder(redStatus);
			reduceBuild.setTaskCompleted(true);
			System.out
					.println("INFO: REDUCER Moving Completed Task from ActiveThreadList to completedThreadList ");
			ReducerThreadPool.activeThreadTask.remove(requiredIndex);
			ReducerThreadPool.completedThreadTask.add(reduceBuild.build());
			System.out.println("INFO: Exit from CS");
		}
	}

}
