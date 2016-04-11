package mapreduce.jobtracker;

import hdfs.Hdfs;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;

import mapreduce.MapReduce;
import mapreduce.MapReduce.MapTaskInfo;
import INameNode.INameNode;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private static final String EQUALS = "=";
	private static final String TASK_TRACKER_IPS = "./config/tasktrackerip.ini";
	private static final String CONFIG_FILE = "./config/config.ini";
	private static final String MAPPERS_REDUCERS = "mappers_reducers.ini";
	private static final long serialVersionUID = 1L;
	private static final int SUCCESS = 1;
	private static final int FAILURE = 0;
	private static final Object jobLock = new Object();
	private static int jobIdCnt = 0;
	private static List<MapReducePair> mappersReducersList;
	private static final Map<Integer, String> tt_id_ip;
	private static final String NAMENODE = "namenode";

	private static String namenodeIp;
	private INameNode namenode;
	private Queue<Job> waitingJobQueue;
	private Map<Integer,List<Integer>> jobTasklistMap;
	private Queue<MapTaskInfo> waitingMapTasks;
	
	

	public JobTracker() throws RemoteException {
		super();
		System.out.println("INFO : Started Job tracker");
		waitingJobQueue = new LinkedList<>();
		connectNameNode();
	}

	static {
		mappersReducersList = new ArrayList<MapReducePair>();
		tt_id_ip = new HashMap<Integer, String>();

		Scanner sc = null;
		String line;
		try {
			sc = new Scanner(new File(MAPPERS_REDUCERS));
			System.out.println("INFO: Loading mappers and reducers..");
			while (sc.hasNext()) {
				line = sc.nextLine();
				String map_red[] = line.split("-");
				mappersReducersList.add(new MapReducePair(map_red[0], map_red[1]));
			}
			sc.close();
			System.out.println("INFO: Loading Task tracker's id and ip..");
			sc = new Scanner(new File(TASK_TRACKER_IPS));
			while (sc.hasNext()) {
				line = sc.nextLine();
				String id_ip[] = line.split(EQUALS);
				tt_id_ip.put(Integer.parseInt(id_ip[0]), id_ip[1]);
			}
			sc.close();
			
			sc = new Scanner(new File(CONFIG_FILE));
			System.out.println("INFO: Loading namenode ip");
			String data[];
			while (sc.hasNext()) {
				data = sc.nextLine().split("=");
				if (NAMENODE.equals(data[0])) {
				 namenodeIp = data[1];
				}
			}
		} catch (IOException e) {
			System.out.println("Error loading init files");
		} finally {
			if (sc != null)
				sc.close();
		}
	}

	private void connectNameNode(){
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			Registry registry = LocateRegistry.getRegistry(namenodeIp);
			namenode = (INameNode) registry.lookup("NameNode");
		} catch ( RemoteException | NotBoundException e) {
			System.out.println("ERROR: Error in connecting to namenode...");
			e.printStackTrace();
		}
	}
	
	@Override
	public byte[] jobSubmit(byte[] jobSubmitRequest) {
		System.out.println("INFO: Submit job request received");
		MapReduce.JobSubmitResponse.Builder jobResponseBuilder = MapReduce.JobSubmitResponse
				.newBuilder();
		try {
			MapReduce.JobSubmitRequest jobSubmit = MapReduce.JobSubmitRequest
					.parseFrom(jobSubmitRequest);
			
			String mapname = jobSubmit.getMapName();
			String reducename = jobSubmit.getReducerName();
			MapReducePair mrPair = new MapReducePair(mapname, reducename);
			if (!mappersReducersList.contains(mrPair)) {
				System.out.println("No mapper-reducer present with given mapname and reduce name");
				jobResponseBuilder.setStatus(FAILURE);
				return jobResponseBuilder.build().toByteArray();
			}

			synchronized (jobLock) {
				jobIdCnt++;
				Job job = new Job(jobIdCnt, jobSubmit.getInputFile(), jobSubmit.getOutputFile(),
						jobSubmit.getNumReduceTasks(), mapname, reducename);
				waitingJobQueue.add(job);
				jobResponseBuilder.setJobId(jobIdCnt);
				jobResponseBuilder.setStatus(SUCCESS);
			}

		} catch (InvalidProtocolBufferException e) {
			System.out.println("ERROR: Not a valid protobuf");
			e.printStackTrace();
		}
		System.out.println("INFO: Sending job response");
		return jobResponseBuilder.build().toByteArray();
	}
	
	private byte[] constructOpen(String fileName, boolean isRead) {
		Hdfs.OpenFileRequest.Builder openBuilder = Hdfs.OpenFileRequest.newBuilder();
		openBuilder.setFileName(fileName);
		openBuilder.setForRead(isRead);
		return openBuilder.build().toByteArray();
	}
	
	private Hdfs.OpenFileResponse open(String fileName, boolean forRead) {
		if (namenode == null) {
			System.out.println("Name node is not connected ");
			return null;
		}
		byte[] openReq = constructOpen(fileName, forRead);
		byte[] responseArray;
		try {
			responseArray = namenode.openFile(openReq);
			Hdfs.OpenFileResponse response = Hdfs.OpenFileResponse.parseFrom(responseArray);
			if (response.getStatus() == FAILURE) {
				System.out.println("Namenode not allowing to open file....");
				return null;
			}
			return response;
		} catch (RemoteException | InvalidProtocolBufferException e) {
			System.out.println("ERROR: Exception in opening request...");
			e.printStackTrace();
			return null;
		}

	}
	
	private void processWaitingQueue(){
		Job job;
		synchronized (jobLock) {
			if(waitingJobQueue.isEmpty()){
				System.out.println("INFO: No jobs in waiting queue");
				return;
			}
			job = waitingJobQueue.poll();
		}
		Hdfs.OpenFileResponse fileInfo = open(job.getInputFileName(), true);
		if(fileInfo == null){
			System.out.println("ERROR: File not present in HDFS");
			//TODO: Inform client the same
			return;
		}
		List<Integer> blockNums = fileInfo.getBlockNumsList();
	
		
		for(int blk : blockNums){
			System.out.println(blk);
		}
		/*
		 * 1. get blocs from namenode
		 * 2. Create n maptasks
		 * 3. put in maptask map
		 * 
		 * */
	}

	@Override
	public byte[] getJobStatus(byte[] jobStatusRequest) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] heartbeatRequest) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) {
		if (System.getSecurityManager() == null)
			System.setSecurityManager(new RMISecurityManager());

		try {
			final JobTracker jobTracker = new JobTracker();
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.bind("JobTracker", jobTracker);
			System.out.println("Service Bound..");
			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
						jobTracker.processWaitingQueue();
					}

				}
			}).start();

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
	}

}

class MapReducePair {

	public String mapper;
	public String reducer;

	public MapReducePair(String map, String reduce) {
		mapper = map;
		reducer = reduce;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mapper == null) ? 0 : mapper.hashCode());
		result = prime * result + ((reducer == null) ? 0 : reducer.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MapReducePair other = (MapReducePair) obj;
		if (mapper == null) {
			if (other.mapper != null)
				return false;
		} else if (!mapper.equals(other.mapper))
			return false;
		if (reducer == null) {
			if (other.reducer != null)
				return false;
		} else if (!reducer.equals(other.reducer))
			return false;
		return true;
	}
}
