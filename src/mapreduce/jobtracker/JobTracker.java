package mapreduce.jobtracker;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import mapreduce.MapReduce;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private static final String EQUALS = "=";
	private static final String TASK_TRACKER_IPS = "./config/tasktrackerip.ini";
	private static final String MAPPERS_REDUCERS = "mappers_reducers.ini";
	private static final long serialVersionUID = 1L;
	private static final int SUCCESS = 1;
	private static final int FAILURE = 0;
	private static final Object jobLock = new Object();
	private static int jobIdCnt = 0;
	private static List<MapReducePair> mappersReducersList;
	private static final Map<Integer,String> tt_id_ip;
	
	public JobTracker() throws RemoteException {
		super();
	}

	static {
		mappersReducersList = new ArrayList<MapReducePair>();
		tt_id_ip = new HashMap<Integer, String>();
		
		Scanner sc = null;
		String line;
		try {
			sc = new Scanner(new File(MAPPERS_REDUCERS));
			while (sc.hasNext()) {
				line = sc.nextLine();
				String map_red[] = line.split("-");
				mappersReducersList.add(new MapReducePair(map_red[0], map_red[1]));
			}
			sc.close();
			sc = new Scanner(new File(TASK_TRACKER_IPS));
			while(sc.hasNext()){
				line = sc.nextLine();
				String id_ip[] = line.split(EQUALS);
				tt_id_ip.put(Integer.parseInt(id_ip[0]), id_ip[1]);
			}
			sc.close();
		} catch (IOException e) {
			System.out.println("Error loading init files");
		} finally{
			if(sc!=null)
				sc.close();
		}
	}
	
	public String getRandomDataNodeIp(){
		int ttCnt  = tt_id_ip.size();
		Random rand = new Random();
		int randId = rand.nextInt(ttCnt)+1;
		return tt_id_ip.get(randId);
	}

	@Override
	public byte[] jobSubmit(byte[] jobSubmitRequest) {

		/*
		 * message JobSubmitRequest { optional string mapName = 1; // Java
		 * class, or name of C .so optional string reducerName = 2; // Java
		 * class, or name of C .so optional string inputFile = 3; optional
		 * string outputFile = 4; optional int32 numReduceTasks = 5; }
		 * 
		 * message JobSubmitResponse { optional int32 status = 1; optional int32
		 * jobId = 2; }
		 */
		try {
			MapReduce.JobSubmitRequest jobSubmit = MapReduce.JobSubmitRequest
					.parseFrom(jobSubmitRequest);
			MapReduce.JobStatusResponse.Builder jobResponseBuilder = MapReduce.JobStatusResponse
					.newBuilder();
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
				Job job = new Job(jobIdCnt, mapname, reducename, jobSubmit.getNumReduceTasks());
				//if(!)
			}
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}

		return null;
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
							Thread.sleep(30000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						// jobTracker.dumpToFile();

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
