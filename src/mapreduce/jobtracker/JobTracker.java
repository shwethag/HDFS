package mapreduce.jobtracker;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;

import com.google.protobuf.InvalidProtocolBufferException;

import INameNode.INameNode;
import hdfs.Hdfs;
import hdfs.Hdfs.BlockLocationRequest;
import hdfs.Hdfs.BlockLocationResponse;
import hdfs.Hdfs.BlockLocations;
import hdfs.Hdfs.DataNodeLocation;
import hdfs.Hdfs.HeartBeatRequest;
import mapreduce.MapReduce;
import mapreduce.MapReduce.HeartBeatResponse;
import mapreduce.MapReduce.JobStatusResponse;
import mapreduce.MapReduce.MapTaskInfo;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private static final String EQUALS = "=";

	private static final String TASK_TRACKER_IPS = "./config/tasktracker.ini";
	private static final String CONFIG_FILE = "./config/config.ini";
	private static final String MAPPERS_REDUCERS = "./config/mappers_reducers.ini";

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
	private Map<Integer,Job> jobInfoMap;
	private Map<Integer, List<Integer>> jobTasklistMap;
	private Queue<MapTaskInfo> waitingMapTasks;
	private Map<Integer, JobStatusResponse.Builder> activeJobMap;
	private Map<Integer, JobStatusResponse> completedJobMap;

	public JobTracker() throws RemoteException {
		super();
		System.out.println("INFO : Started Job tracker");
		waitingJobQueue = new LinkedList<>();
		jobTasklistMap = new HashMap<>();
		waitingMapTasks = new LinkedList<>();
		activeJobMap = new HashMap<>();
		completedJobMap = new HashMap<>();
		jobInfoMap = new HashMap<>();
		// connectNameNode();
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
			e.printStackTrace();
		} finally {
			if (sc != null)
				sc.close();
		}
	}

	private void connectNameNode() {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			Registry registry = LocateRegistry.getRegistry(namenodeIp);
			System.out.println("INFO: NameNode IP:" + namenodeIp);
			namenode = (INameNode) registry.lookup("NameNode");
		} catch (RemoteException | NotBoundException e) {
			System.out.println("ERROR: Error in connecting to namenode...");
			e.printStackTrace();
		}
	}

	@Override
	public byte[] jobSubmit(byte[] jobSubmitRequest) throws RemoteException {
		System.out.println("INFO: Submit job request received");
		MapReduce.JobSubmitResponse.Builder jobResponseBuilder = MapReduce.JobSubmitResponse.newBuilder();
		try {
			MapReduce.JobSubmitRequest jobSubmit = MapReduce.JobSubmitRequest.parseFrom(jobSubmitRequest);

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
				jobInfoMap.put(jobIdCnt, job);
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
		System.out.println("Opening file in HDFS..");
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

	private mapreduce.MapReduce.BlockLocations copyBlockLocations(BlockLocations blkLocation) {
		System.out.println("INFO: Copying block locations..");
		mapreduce.MapReduce.BlockLocations.Builder mpBlkLocation = mapreduce.MapReduce.BlockLocations.newBuilder();
		mpBlkLocation.setBlockNumber(blkLocation.getBlockNumber());
		for (DataNodeLocation dloc : blkLocation.getLocationsList()) {
			System.out.println("DN: IP=" + dloc.getIp());
			mapreduce.MapReduce.DataNodeLocation.Builder mpDlocBuilder = mapreduce.MapReduce.DataNodeLocation
					.newBuilder();

			mpDlocBuilder.setIp(dloc.getIp());
			mpDlocBuilder.setPort(dloc.getPort());
			mpBlkLocation.addLocations(mpDlocBuilder.build());

		}
		return mpBlkLocation.build();
	}

	private void processWaitingQueue() {
		System.out.println("INFO: Processing waiting queue");
		Job job;
		synchronized (jobLock) {
			if (waitingJobQueue.isEmpty()) {
				System.out.println("INFO: No jobs in waiting queue");
				return;
			}
			job = waitingJobQueue.poll();
		}
		Hdfs.OpenFileResponse fileInfo = open(job.getInputFileName(), true);
		if (fileInfo == null) {
			System.out.println("ERROR: File not present in HDFS");
			// TODO: Inform client the same
			return;
		}
		System.out.println("INFO: Getting block numbers list");
		List<Integer> blockNums = fileInfo.getBlockNumsList();

		BlockLocationRequest.Builder blkLocReqBuilder = BlockLocationRequest.newBuilder();
		blkLocReqBuilder.addAllBlockNums(blockNums);

		try {
			System.out.println("INFO: Getting Block locations..");
			byte[] blkReqResponse = namenode.getBlockLocations(blkLocReqBuilder.build().toByteArray());
			BlockLocationResponse blkResp = BlockLocationResponse.parseFrom(blkReqResponse);
			int taskId = 1;
			System.out.println("INFO: Processing block location response..");
			for (BlockLocations blkLocation : blkResp.getBlockLocationsList()) {
				List<Integer> tidlist = null;
				if (jobTasklistMap.containsKey(job.getJobId())) {
					tidlist = jobTasklistMap.get(job.getJobId());

				} else {
					tidlist = new ArrayList<Integer>();
				}
				tidlist.add(taskId);
				jobTasklistMap.put(job.getJobId(), tidlist);
				mapreduce.MapReduce.BlockLocations mpBlkLocation = copyBlockLocations(blkLocation);
				MapTaskInfo.Builder mapTaskBuilder = MapTaskInfo.newBuilder();
				mapTaskBuilder.setJobId(job.getJobId());
				mapTaskBuilder.setTaskId(taskId);
				mapTaskBuilder.setMapName(job.getMapName());
				mapTaskBuilder.addInputBlocks(mpBlkLocation);
				waitingMapTasks.add(mapTaskBuilder.build());
				taskId++;
			}
		} catch (RemoteException | InvalidProtocolBufferException e) {
			System.out.println("ERROR: Failed to get block locations..");
			e.printStackTrace();
			return;
		}
	}

	@Override
	public byte[] getJobStatus(byte[] jobStatusRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	private HeartBeatResponse.Builder processWaitingMapTask(HeartBeatResponse.Builder heartBeatResponse,
			mapreduce.MapReduce.HeartBeatRequest heartBeatRequest, int freeMapSlots)
					throws InvalidProtocolBufferException {
		System.out.println("INFO: Free Map Slots available is " + freeMapSlots);
		if (freeMapSlots != 0 && !waitingMapTasks.isEmpty()) {
			System.out.println("INFO: Assigning map Task to " + heartBeatRequest.getTaskTrackerId());
			int count = Math.min(freeMapSlots, waitingMapTasks.size());
			heartBeatResponse.setStatus(SUCCESS);
			for (int i = 0; i < count; i++) {
				// pop from waiting queue
				MapTaskInfo maptask = waitingMapTasks.poll();
				// add it in heartbeat response
				heartBeatResponse.addMapTasks(maptask);
				// add it to active map
				int jobId = maptask.getJobId();
				if (activeJobMap.containsKey(jobId)) {
					// need to modify the values
					System.out.println("INFO: Modifying the jobStatusResponse node for jobId: "+jobId);
					JobStatusResponse.Builder jobStatusResponse = activeJobMap.get(jobId);
					jobStatusResponse.setNumMapTasksStarted(jobStatusResponse.getNumMapTasksStarted()+1);
					activeJobMap.put(jobId, jobStatusResponse);
					
					
					
				} else {
					// add new pair to map
					System.out.println("INFO: Adding new pair to active map ");
					JobStatusResponse.Builder jobStatusResponse = JobStatusResponse.newBuilder();
					jobStatusResponse.setJobDone(false);
					int totalMapTask = jobTasklistMap.get(jobId).size();
					jobStatusResponse.setTotalMapTasks(totalMapTask);
					jobStatusResponse.setNumMapTasksStarted(1);
					jobStatusResponse.setTotalReduceTasks(jobInfoMap.get(jobId).getReducersCnt());
					jobStatusResponse.setNumReduceTasksStarted(0);
					activeJobMap.put(jobId, jobStatusResponse);
				}

			}
		} else {
			System.out.println("INFO: No map Task was assigned to " + heartBeatRequest.getTaskTrackerId());
		}
		return heartBeatResponse;
	}

	@Override
	public byte[] heartBeat(byte[] heartbeatRequestByte) throws RemoteException {

		HeartBeatResponse.Builder heartBeatResponse = HeartBeatResponse.newBuilder();
		try {
			mapreduce.MapReduce.HeartBeatRequest heartBeatRequest = mapreduce.MapReduce.HeartBeatRequest
					.parseFrom(heartbeatRequestByte);
			System.out.println("INFO: Recieved heartbeat request from: " + heartBeatRequest.getTaskTrackerId());
			int freeMapSlots = heartBeatRequest.getNumMapSlotsFree();
			heartBeatResponse = processWaitingMapTask(heartBeatResponse, heartBeatRequest, freeMapSlots);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		if (System.getSecurityManager() == null)
			System.setSecurityManager(new RMISecurityManager());

		try {
			Registry registry;
			final JobTracker jobTracker = new JobTracker();
			try {
				registry = LocateRegistry.createRegistry(1099);
			} catch (ExportException e) {
				System.out.println("Registry already created.. getting the registry");
				registry = LocateRegistry.getRegistry(1099);
			}
			registry.bind("JobTracker", jobTracker);
			System.out.println("Service Bound..");
			jobTracker.connectNameNode();
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
