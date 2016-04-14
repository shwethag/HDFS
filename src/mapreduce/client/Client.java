package mapreduce.client;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

import mapreduce.MapReduce;
import mapreduce.MapReduce.JobStatusRequest;
import mapreduce.MapReduce.JobStatusResponse;
import mapreduce.jobtracker.IJobTracker;

import com.google.protobuf.InvalidProtocolBufferException;

public class Client {
	
	private static String jobTrackerIP;
	private IJobTracker jobTracker=null;
	private static final String CONFIG = "./config/config.ini";
	private static final String JOB_TRACKER="jobtracker";
	private String mapName,reducerName,inputFileinHdfs,outputFileinHdfs;
	private int numOfReducers;
	 
	public Client(String mapName,String reduceName,String inpFile,String outFile,int numReducers){
		System.out.println("Info: Connecting to Jobtracker:"+jobTrackerIP);
		connectToJobTracker();
		this.mapName=mapName;
		this.reducerName=reduceName;
		this.inputFileinHdfs=inpFile;
		this.outputFileinHdfs=outFile;
		this.numOfReducers=numReducers;
		System.out.println("INFO:JobTracker Connection was successfull");
	}
	
	
	static {
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
	
	private void printStatus(float percent){
		System.out.println("Percentage Completion.. " + percent);
		//percent=percent/10;
		
		for(int i=0;i<=101;i++){
			if(i==0 || i==101){
				System.out.print("||");
			}else if(i<=percent){
				System.out.print("=");
			}else{
				System.out.print(" ");
			}
		}
		System.out.println();
	}
	
	private int submitJobRequest(){
		System.out.println("INFO: Submit the job request");
		MapReduce.JobSubmitRequest.Builder jobSubmitRequest = MapReduce.JobSubmitRequest.newBuilder();
		jobSubmitRequest.setMapName(mapName);
		jobSubmitRequest.setReducerName(reducerName);
		jobSubmitRequest.setInputFile(inputFileinHdfs);
		jobSubmitRequest.setOutputFile(outputFileinHdfs);
		jobSubmitRequest.setNumReduceTasks(numOfReducers);
		int jobId=-1;
		byte[] jobSubmitResponseByte;
		try {
			jobSubmitResponseByte = jobTracker.jobSubmit(jobSubmitRequest.build().toByteArray());
		
			MapReduce.JobSubmitResponse jobSubmitResponse = MapReduce.JobSubmitResponse.parseFrom(jobSubmitResponseByte);
			jobId=jobSubmitResponse.getJobId();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		return jobId;
	}
	
	public void checkForJobStatus(int jobId){
		JobStatusRequest.Builder jobStatusReqBuilder = JobStatusRequest.newBuilder();
		jobStatusReqBuilder.setJobId(jobId);
		byte [] jobStatusReq = jobStatusReqBuilder.build().toByteArray(); 
		while(true){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			try {
				byte[] jobStatusRespByte = jobTracker.getJobStatus(jobStatusReq);
				if(jobStatusRespByte == null){
					System.out.println("ERROR: No response received..");
					continue;
				}
				JobStatusResponse jobStatusResponse = JobStatusResponse.parseFrom(jobStatusRespByte);
				
				int mapTasksCnt = jobStatusResponse.getTotalMapTasks();
				int redTaskCnt = jobStatusResponse.getTotalReduceTasks();
				int mapTaskStarted = jobStatusResponse.getNumMapTasksStarted();
				int redTaskStarted = jobStatusResponse.getNumReduceTasksStarted();
				Runtime.getRuntime().exec("clear");
				System.out.println("***Map Task Status****");
				System.out.print("#MapTasks " + mapTasksCnt + " #MapTasksStarted " + mapTaskStarted + "\t");
				printStatus(mapTaskStarted/(float)mapTasksCnt*100);
				System.out.println();
				
				System.out.println("***Reduce Task Status****");
				System.out.print("#ReduceTasks " + redTaskCnt + " #ReduceTaskStarted " + redTaskStarted+"\t");
				printStatus(redTaskStarted/(float)redTaskCnt*100);
				System.out.println();
				
				if(jobStatusResponse.getJobDone()){
					System.out.println("*****JOB is Completed *********" + jobId);
					break;
				}
			} catch (RemoteException | InvalidProtocolBufferException e) {
				System.out.println("ERROR: connection failure");
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	public static void main(String[] args) {
		if(args.length!=5){
			System.out.println("ERROR: Invalid number of arguments " +
					"<mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers>");
			return;
		}
		
		Scanner sc = null;
		String mapName=args[0];
		String reduceName = args[1];
		String inpFile = args[2];
		String outFile = args[3];
		int numReducers = Integer.parseInt(args[4]);
		Client client = new Client(mapName,reduceName,inpFile,outFile,numReducers);
		
		/*
		 * To Submit the Job request
		 */
		int jobId=client.submitJobRequest();
		if(jobId==-1){
			System.out.println("ERROR: Submission Of JobRequest Failed");
			return;
		}
		
		System.out.println("INFO: JOB Submission Successfull :"+jobId);
		
		/*
		 * To get The job Status
		 */
		
		client.checkForJobStatus(jobId);
		
		
		
		
		
	}
}
