package mapreduce.jobtracker;

import hdfs.Hdfs;
import hdfs.Hdfs.BlockLocationRequest;
import hdfs.Hdfs.BlockLocationResponse;
import hdfs.Hdfs.BlockLocations;
import hdfs.Hdfs.DataNodeLocation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
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

import mapreduce.MapReduce;
import mapreduce.MapReduce.HeartBeatResponse;
import mapreduce.MapReduce.JobStatusRequest;
import mapreduce.MapReduce.JobStatusResponse;
import mapreduce.MapReduce.MapTaskInfo;
import mapreduce.MapReduce.MapTaskStatus;
import mapreduce.MapReduce.ReduceTaskStatus;
import mapreduce.MapReduce.ReducerTaskInfo;
import util.Connector;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private static final String EQUALS = "=";

	private static final String TASK_TRACKER_IPS = "./config/tasktracker.ini";
	private static final String MAPPERS_REDUCERS = "./config/mappers_reducers.ini";
	private static final String JOBID_DUMP = "./data_dump/jobId.dat";

	private static final long serialVersionUID = 1L;
	private static final int SUCCESS = 1;
	private static final int FAILURE = 0;

	private static final Object jobLock = new Object();

	private static int jobIdCnt = 0;

	private static List<MapReducePair> mappersReducersList;
	private static final Map<Integer, String> tt_id_ip;

	private Connector connector;
	private Queue<Job> waitingJobQueue;
	private Map<Integer, Job> jobInfoMap; // jobid,job
	private Map<Integer, List<Integer>> jobMapTasklistMap; // JOBID, Task id
															// list
	private Map<Integer, List<Integer>> jobReduceTasklistMap;
	private Queue<MapTaskInfo> waitingMapTasks;
	private Queue<ReducerTaskInfo> waitingReduceTasks;
	private Map<Integer, JobStatusResponse.Builder> activeMapperJobMap; // JOBID,
																		// JOB
	// STATUS
	private Map<Integer, JobStatusResponse.Builder> activeReducerJobMap; // JOBID,
																			// JOB
	// STATUS

	private Map<Integer, JobStatusResponse.Builder> completedJobMap; // JOBID,
																		// JOB
	// STATUS
	private Map<Integer, List<String>> jobOpFileList; // JOBID, OPFILE of tasks

	public JobTracker() throws RemoteException {
		super();
		System.out.println("INFO : Started Job tracker");
		connector = Connector.getConnector();
		waitingJobQueue = new LinkedList<>();
		jobMapTasklistMap = new HashMap<>();
		jobReduceTasklistMap = new HashMap<>();
		waitingMapTasks = new LinkedList<>();
		waitingReduceTasks = new LinkedList<>();
		activeMapperJobMap = new HashMap<>();
		completedJobMap = new HashMap<>();
		jobInfoMap = new HashMap<>();
		jobOpFileList = new HashMap<>();
		activeReducerJobMap = new HashMap<>();
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
			
			sc = new Scanner(new File(JOBID_DUMP));
			if(sc.hasNext()){
				jobIdCnt=Integer.parseInt(sc.nextLine());
				
			}
			

		} catch (IOException e) {
			System.out.println("Error loading init files");
			e.printStackTrace();
		} finally {
			if (sc != null)
				sc.close();
		}
	}
	
	
	public void dumpJobIdToFile(){
		PrintWriter pr = null;
		
		try {
			pr = new PrintWriter(new File(JOBID_DUMP));
			pr.println(jobIdCnt);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(pr!=null)
				pr.close();
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
				// add new pair to map
				int jobId = jobIdCnt;
				System.out.println("INFO: Adding new pair to active map ");
				JobStatusResponse.Builder jobStatusResponse = JobStatusResponse.newBuilder();
				jobStatusResponse.setJobDone(false);
				// int totalMapTask = jobTasklistMap.get(jobId).size();
				// jobStatusResponse.setTotalMapTasks(totalMapTask);
				jobStatusResponse.setNumMapTasksStarted(0);
				// jobStatusResponse.setTotalReduceTasks(jobInfoMap.get(jobId).getReducersCnt());
				jobStatusResponse.setNumReduceTasksStarted(0);
				activeMapperJobMap.put(jobId, jobStatusResponse);
				jobResponseBuilder.setJobId(jobIdCnt);
				jobInfoMap.put(jobIdCnt, job);
				jobOpFileList.put(jobId, new ArrayList<String>());
				dumpJobIdToFile();
				jobResponseBuilder.setStatus(SUCCESS);
			}

		} catch (InvalidProtocolBufferException e) {
			System.out.println("ERROR: Not a valid protobuf");
			e.printStackTrace();
		}
		System.out.println("INFO: Sending job response");
		return jobResponseBuilder.build().toByteArray();
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
		// System.out.println("INFO: Processing waiting queue");
		Job job;
		synchronized (jobLock) {
			if (waitingJobQueue.isEmpty()) {
				// System.out.println("INFO: No jobs in waiting queue");
				return;
			}
			job = waitingJobQueue.poll();
			System.out.println("INFO: New job found.. " + job.getJobId());
		}
		Hdfs.OpenFileResponse fileInfo = connector.open(job.getInputFileName(), true);
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
			byte[] blkReqResponse = connector.namenode.getBlockLocations(blkLocReqBuilder.build().toByteArray());
			BlockLocationResponse blkResp = BlockLocationResponse.parseFrom(blkReqResponse);
			int taskId = 1;
			System.out.println("INFO: Processing block location response..");
			for (BlockLocations blkLocation : blkResp.getBlockLocationsList()) {
				List<Integer> tidlist = null;
				if (jobMapTasklistMap.containsKey(job.getJobId())) {
					tidlist = jobMapTasklistMap.get(job.getJobId());

				} else {
					tidlist = new ArrayList<Integer>();
				}
				tidlist.add(taskId);
				jobMapTasklistMap.put(job.getJobId(), tidlist);
				mapreduce.MapReduce.BlockLocations mpBlkLocation = copyBlockLocations(blkLocation);
				MapTaskInfo.Builder mapTaskBuilder = MapTaskInfo.newBuilder();
				mapTaskBuilder.setJobId(job.getJobId());
				mapTaskBuilder.setTaskId(taskId);
				mapTaskBuilder.setMapName(job.getMapName());
				mapTaskBuilder.addInputBlocks(mpBlkLocation);
				waitingMapTasks.add(mapTaskBuilder.build());
				taskId++;
			}
			JobStatusResponse.Builder jobBuilder = activeMapperJobMap.get(job.getJobId());
			jobBuilder.setTotalMapTasks(taskId - 1);
			jobBuilder.setTotalReduceTasks(jobInfoMap.get(job.getJobId()).getReducersCnt());
			activeMapperJobMap.put(job.getJobId(), jobBuilder);
		} catch (RemoteException | InvalidProtocolBufferException e) {
			System.out.println("ERROR: Failed to get block locations..");
			e.printStackTrace();
			return;
		}
	}

	@Override
	public byte[] getJobStatus(byte[] jobStatusRequestByte) throws RemoteException {

		JobStatusResponse.Builder jobStatusBuilder = JobStatusResponse.newBuilder();

		try {
			JobStatusRequest jobStatusRequest = JobStatusRequest.parseFrom(jobStatusRequestByte);
			int jobId = jobStatusRequest.getJobId();
			System.out.println("INFO: Client is asking for job status  " + jobId);
			if (completedJobMap.containsKey(jobId)) {
				jobStatusBuilder = completedJobMap.get(jobId);
				jobStatusBuilder.setStatus(SUCCESS);
				jobStatusBuilder.setJobDone(true);
			} else if (activeMapperJobMap.containsKey(jobId)) {
				jobStatusBuilder = activeMapperJobMap.get(jobId);
				jobStatusBuilder.setStatus(SUCCESS);
			} else if (activeReducerJobMap.containsKey(jobId)) {
				jobStatusBuilder = activeReducerJobMap.get(jobId);
				jobStatusBuilder.setStatus(SUCCESS);
			} else {
				jobStatusBuilder.setStatus(FAILURE);
			}

		} catch (InvalidProtocolBufferException e) {
			System.out.println("ERROR: Could not deserialize");
			jobStatusBuilder.setStatus(FAILURE);
			e.printStackTrace();

		}
		System.out.println("DEBUG: " + jobStatusBuilder.getStatus() + " " + jobStatusBuilder.getNumMapTasksStarted());
		return jobStatusBuilder.build().toByteArray();
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
				// need to modify the values
				System.out.println("INFO: Modifying the jobStatusResponse node for jobId: " + jobId);
				JobStatusResponse.Builder jobStatusResponse = activeMapperJobMap.get(jobId);
				jobStatusResponse.setNumMapTasksStarted(jobStatusResponse.getNumMapTasksStarted() + 1);
				activeMapperJobMap.put(jobId, jobStatusResponse);
			}
		} else {
			//System.out.println("INFO: No map Task was assigned to " + heartBeatRequest.getTaskTrackerId());
		}
		return heartBeatResponse;
	}

	private HeartBeatResponse.Builder processWaitingReduceTask(HeartBeatResponse.Builder heartBeatResponse,
			mapreduce.MapReduce.HeartBeatRequest heartBeatRequest, int freeReduceSlots)
					throws InvalidProtocolBufferException {
		System.out.println("INFO: Processing waiting reduce task");
		System.out.println(
				"DEBUG: Free reduce Task:" + freeReduceSlots + "  reduce queue Size:" + waitingReduceTasks.size());
		if (freeReduceSlots != 0 && !waitingReduceTasks.isEmpty()) {
			System.out.println("INFO: Assigning reduce Task to " + heartBeatRequest.getTaskTrackerId());
			int count = Math.min(freeReduceSlots, waitingReduceTasks.size());
			heartBeatResponse.setStatus(SUCCESS);
			for (int i = 0; i < count; i++) {
				// pop from waiting queue
				ReducerTaskInfo reducetask = waitingReduceTasks.poll();
				// add it in heartbeat response
				heartBeatResponse.addReduceTasks(reducetask);
				// add it to active map
				int jobId = reducetask.getJobId();
				// need to modify the values
				System.out.println("INFO: Modifying the jobStatusResponse node for jobId: " + jobId);
				JobStatusResponse.Builder jobStatusResponse = activeReducerJobMap.get(jobId);
				jobStatusResponse.setNumReduceTasksStarted(jobStatusResponse.getNumReduceTasksStarted() + 1);
				activeReducerJobMap.put(jobId, jobStatusResponse);
			}
		} else {
			//System.out.println("INFO: No map Task was assigned to " + heartBeatRequest.getTaskTrackerId());
		}
		return heartBeatResponse;

	}

	private void createReduceTasks(int jobId) {
		System.out.println("INFO: Creating reduce tasks for job " + jobId);
		int totalOpFile = jobOpFileList.get(jobId).size();
		int reduceTaskCnt = jobInfoMap.get(jobId).getReducersCnt();
		int grpCnt =  totalOpFile / reduceTaskCnt;
		System.out.println("DEBUG: ReduceTaskCnt="+reduceTaskCnt+" TotalOpFile="+totalOpFile+" grpCnt="+grpCnt);
		int cnt = 0;
		List<String> opFileList = jobOpFileList.get(jobId);
		List<String> mapOutputFiles = new ArrayList<>();
		jobReduceTasklistMap.put(jobId, new ArrayList<Integer>());
		int taskid = 1;
		int curRedceCnt=0;
		List<Integer> taskIdList = new ArrayList<>();
		for (int i = 0; i < totalOpFile; i++) {
			if (cnt == grpCnt && curRedceCnt<reduceTaskCnt-1) {
				curRedceCnt++;
				ReducerTaskInfo.Builder reduceTaskBuilder = ReducerTaskInfo.newBuilder();
				reduceTaskBuilder.setJobId(jobId);
				reduceTaskBuilder.setTaskId(taskid);
				reduceTaskBuilder.setReducerName(jobInfoMap.get(jobId).getReduceName());
				reduceTaskBuilder.addAllMapOutputFiles(mapOutputFiles);
				reduceTaskBuilder.setOutputFile(jobInfoMap.get(jobId).getOutputFileName() + "_" + jobId + "_" + taskid);
				System.out.println("Adding reduce task to waiting queue " + jobId + " " + taskid);
				waitingReduceTasks.add(reduceTaskBuilder.build());
				taskIdList.add(taskid);
				taskid++;
				mapOutputFiles = new ArrayList<>();
				cnt = 0;
			} 
				cnt++;
				mapOutputFiles.add(opFileList.get(i));
			
		}

		if (mapOutputFiles.size() > 0) {

			ReducerTaskInfo.Builder reduceTaskBuilder = ReducerTaskInfo.newBuilder();
			reduceTaskBuilder.setJobId(jobId);
			reduceTaskBuilder.setTaskId(taskid);
			reduceTaskBuilder.setReducerName(jobInfoMap.get(jobId).getReduceName());
			reduceTaskBuilder.addAllMapOutputFiles(mapOutputFiles);
			reduceTaskBuilder.setOutputFile(jobInfoMap.get(jobId).getOutputFileName() + "_" + jobId + "_" + taskid);
			taskIdList.add(taskid);
			System.out.println("Adding last reduce task to waiting queue " + jobId + " " + taskid);
			waitingReduceTasks.add(reduceTaskBuilder.build());
		}
		System.out.println("DEBUG: Total Tasks for job ID "+jobId + " is:"+taskIdList.size());
		System.out.println("DEBUG: waitingReduce Size:"+waitingReduceTasks.size());
		jobReduceTasklistMap.put(jobId, taskIdList);

	}

	private void processMapTaskStatus(List<MapTaskStatus> mapStatusList) {
		//System.out.println("INFO: Processing map task status from HB");
		for (MapTaskStatus mpStatus : mapStatusList) {
			if (mpStatus.getTaskCompleted()) {
				int jobId = mpStatus.getJobId();
				System.out.println(
						"INFO: Job with id " + jobId + " and Map task id " + mpStatus.getTaskId() + " is completed");

				List<String> opFileList = jobOpFileList.get(jobId);
				opFileList.add(mpStatus.getMapOutputFile());
				jobOpFileList.put(jobId, opFileList);
				List<Integer> taskList = jobMapTasklistMap.get(jobId);
				taskList.remove((Integer) mpStatus.getTaskId());
				if (taskList.isEmpty()) {
					System.out.println("INFO: All Map tasks completed for job " + jobId);
					JobStatusResponse.Builder jobStatusBuilder = activeMapperJobMap.get(jobId);
					activeMapperJobMap.remove(jobId);
					System.out.println("INFO: Moving job to active reducer map " + jobId);
					activeReducerJobMap.put(jobId, jobStatusBuilder);
					createReduceTasks(jobId);

				}
				jobMapTasklistMap.put(jobId, taskList);

			}
		}
	}

	private void processReduceTaskStatus(List<ReduceTaskStatus> reduceStatusList) {
		System.out.println("INFO: Processing reduce task status from HB");
		for (ReduceTaskStatus reduceTaskStatus : reduceStatusList) {
			if (reduceTaskStatus.getTaskCompleted()) {
				int jobId = reduceTaskStatus.getJobId();
				System.out.println("INFO: Job with id " + jobId + " and reduce task id " + reduceTaskStatus.getTaskId()
						+ " is completed");

				// List<String> opFileList = jobOpFileList.get(jobId);
				// opFileList.add(mpStatus.getMapOutputFile());
				// jobOpFileList.put(jobId, opFileList);
				List<Integer> taskList = jobReduceTasklistMap.get(jobId);
				taskList.remove((Integer) reduceTaskStatus.getTaskId());
				if (taskList.isEmpty()) {
					System.out.println("INFO: All Reduce tasks completed for job " + jobId);
					JobStatusResponse.Builder jobStatusBuilder = activeReducerJobMap.get(jobId);
					activeReducerJobMap.remove(jobId);
					System.out.println("INFO: Moving job to completed Job map " + jobId);
					completedJobMap.put(jobId, jobStatusBuilder);

				}
				jobReduceTasklistMap.put(jobId, taskList);

			}
		}
	}

	@Override
	public byte[] heartBeat(byte[] heartbeatRequestByte) throws RemoteException {

		HeartBeatResponse.Builder heartBeatResponse = HeartBeatResponse.newBuilder();
		try {
			mapreduce.MapReduce.HeartBeatRequest heartBeatRequest = mapreduce.MapReduce.HeartBeatRequest
					.parseFrom(heartbeatRequestByte);
					// System.out.println("INFO: Recieved heartbeat request
					// from: "
					// + heartBeatRequest.getTaskTrackerId());
					/******* Process 1: To handle map status response ********/
			processMapTaskStatus(heartBeatRequest.getMapStatusList());
			/******* Process 2: To handle reduce status response ********/
			processReduceTaskStatus(heartBeatRequest.getReduceStatusList());
			/******* Process 3: To assign new map tasks ********/
			int freeMapSlots = heartBeatRequest.getNumMapSlotsFree();
			heartBeatResponse = processWaitingMapTask(heartBeatResponse, heartBeatRequest, freeMapSlots);
			/******* Process 4: To assign new reduce tasks ********/
			int freeReduceSlots = heartBeatRequest.getNumReduceSlotsFree();
			heartBeatResponse = processWaitingReduceTask(heartBeatResponse, heartBeatRequest, freeReduceSlots);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			heartBeatResponse.setStatus(FAILURE);
			return heartBeatResponse.build().toByteArray();
		}
		heartBeatResponse.setStatus(SUCCESS);
		return heartBeatResponse.build().toByteArray();
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
			// jobTracker.connector.connectNameNode();
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
