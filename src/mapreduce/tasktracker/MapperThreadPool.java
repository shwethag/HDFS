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
import com.google.protobuf.InvalidProtocolBufferException;

import IDataNode.IDataNode;
import INameNode.INameNode;
import hdfs.Hdfs;
import hdfs.Client;
import mapreduce.IMapper;
import mapreduce.MapReduce.BlockLocations;
import mapreduce.MapReduce.DataNodeLocation;
import mapreduce.MapReduce.MapTaskInfo;
import mapreduce.MapReduce.MapTaskStatus;
import util.Connector;

public class MapperThreadPool {
	private static final String SEARCH_FILE = "./config/searchword.ini";
	private static final String CONFIG = "./config/config.ini";
	private static final String BLOCK = "blocksize";
	private static final String NAMENODE = "namenode";
	private static int blockSize;
	private static String namenodeIp;
	private static INameNode namenode;
	
	public static String searchterm;
	public static int maxThreadCount = 5;
	public static int availableCount = maxThreadCount;
	public static Object lock = new Object();
	public static List<MapTaskStatus> activeThreadTask = null;
	public static List<MapTaskStatus> completedThreadTask = null;
	private ExecutorService mapExecutor = null;
	
	
	static{
		Scanner sc = null;
		String data[];
		try {
			sc = new Scanner(new File(SEARCH_FILE));
			if (sc.hasNext()) {
				searchterm = sc.nextLine();
			}
			sc.close();
			sc = new Scanner(new File(CONFIG));
			while (sc.hasNext()) {
				data = sc.nextLine().split("=");
				if (BLOCK.equals(data[0])) {
					blockSize = Integer.parseInt(data[1]);
					System.out.println("INFO: block size " + blockSize);
				} else if (NAMENODE.equals(data[0])) {
					namenodeIp = data[1];
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (sc != null)
				sc.close();
		}
		
		
		//To Connect to Name Node
	}
	
	

	public MapperThreadPool() {
		System.out.println("INFO: Mapper Thread Constructor Started");
		activeThreadTask = new ArrayList<>();
		completedThreadTask = new ArrayList<>();
		mapExecutor = Executors.newFixedThreadPool(maxThreadCount);
		

	}

	public void addNewMapTask(MapTaskInfo mapTask) {
		Runnable worker = new MapWorker(mapTask);
		mapExecutor.execute(worker);
	}

	public static void flushCompletedList() {
		synchronized (lock) {
			completedThreadTask.clear();
		}
	}

}

class MapWorker implements Runnable {
	private static final String NEWLINE = "\n";
	private static final int FAILURE = 0;
	private MapTaskInfo mapTaskInfo;
	private Connector connector;

	public MapWorker(MapTaskInfo mapTaskInfo) {
		this.mapTaskInfo = mapTaskInfo;
		connector = Connector.getConnector();
	}

	private IDataNode connectDatanode(DataNodeLocation datanodeAddress) {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			// TODO: Check for port usage
			Registry registry = LocateRegistry.getRegistry(datanodeAddress.getIp());
			IDataNode datanode = (IDataNode) registry.lookup("DataNode");
			return datanode;
		} catch (RemoteException | NotBoundException e) {
			System.out.println("ERROR: Error in connect datanode request...Returning......");
			e.printStackTrace();
			return null;
		}

	}

	public void handleMapFunction(ByteString blockString) {
		// to Create a local File
		System.out.println("INFO: Map task is initiated");
		String fileName = "job_" + mapTaskInfo.getJobId() + "_map_" + mapTaskInfo.getTaskId();
		System.out.println("INFO: Grep temp results will be dumped in " + fileName);
		PrintWriter pr = null;
		try {
			pr = new PrintWriter(new File(fileName));
			System.out.println("INFO: to bind class dynamically: "+mapTaskInfo.getMapName());
			Class cls = Class.forName(mapTaskInfo.getMapName());
			IMapper mapper = (IMapper) cls.newInstance();

			String[] lines = blockString.toStringUtf8().split(NEWLINE);
			pr.println(fileName);
			for (String line : lines) {
				String res = mapper.map(line,MapperThreadPool.searchterm);
				if (res != null)
					pr.println(res);
			}
		} catch (FileNotFoundException | ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} finally {
			if (pr != null)
				pr.close();
		}
	}
	
	public void dumpMapFileToHDFS(){
		String fileName = "job_" + mapTaskInfo.getJobId() + "_map_" + mapTaskInfo.getTaskId();
		System.out.println("INFO: Dumping file: "+fileName+"");
		connector.put(fileName);
	}

	public void executeMapTask() {
		// Connect to data Node to get Block
		ByteString s = null;
		System.out.println("INFO: GetBlock locations");
		BlockLocations blockLocations = mapTaskInfo.getInputBlocks(0);
		IDataNode dataNode = null;
		for (DataNodeLocation daLocation : blockLocations.getLocationsList()) {
			System.out.println("INFO: tryig to connect to dataNode");
			dataNode = connectDatanode(daLocation);
			if (dataNode == null) {
				System.out.println("INFO: Connection was NOT successfull: " + daLocation.getIp());
				continue;
			}
			// To read Block
			Hdfs.ReadBlockRequest.Builder readBlockReq = Hdfs.ReadBlockRequest.newBuilder();
			readBlockReq.setBlockNumber(blockLocations.getBlockNumber());
			try {
				byte[] readBlockRespArray = dataNode.readBlock(readBlockReq.build().toByteArray());
				Hdfs.ReadBlockResponse readBlockResp = Hdfs.ReadBlockResponse
						.parseFrom(readBlockRespArray);
				if (readBlockResp.getStatus() == FAILURE) {
					System.out.println("WARN: Failed to read from datanode");
					continue;
				}

				for (ByteString content : readBlockResp.getDataList()) {
					s = content;
					break;
					// TODO: Check the proper get methods
				}
				// System.out.println("In Client : " + s.toStringUtf8());
				System.out.println("INFO: Read block successfully.. "
						+ blockLocations.getBlockNumber());
				break;

			} catch (RemoteException | InvalidProtocolBufferException e) {
				System.out.println("ERROR: Read block failed..");
				e.printStackTrace();
			}

		}

		// For each Line in call map() in Mapper.java
		//TODO: verify its correct o not
		handleMapFunction(s);
		// Store the output in a file
		// Once done upload the result file to HDFS and return the file
		dumpMapFileToHDFS();
		
		// information in mapTaskStatus

	}

	@Override
	public void run() {

		synchronized (MapperThreadPool.lock) {
			System.out.println("INFO: Inside Critical Section");
			System.out.println("INFO: reducing available count of mapthread to: "
					+ (MapperThreadPool.availableCount - 1));
			MapperThreadPool.availableCount--;
			// Build mapTaskStatusBuilder
			System.out.println("INFO: Building MaptaskStatus");
			MapTaskStatus.Builder mapTaskStatusBuilder = MapTaskStatus.newBuilder();
			mapTaskStatusBuilder.setJobId(mapTaskInfo.getJobId());
			mapTaskStatusBuilder.setTaskId(mapTaskInfo.getTaskId());
			mapTaskStatusBuilder.setTaskCompleted(false);

			// add this above to active map
			System.out.println("INFO: Added the mapStatusTask to ActiveThread");
			MapperThreadPool.activeThreadTask.add(mapTaskStatusBuilder.build());
			System.out.println("INFO: Exit from critical Section");
		}
		System.out.println("INFO: Execution Of Map Task Started");
		executeMapTask();
		System.out.println("INFO: Execution Of Map Task Completed");

		synchronized (MapperThreadPool.lock) {
			System.out.println("INFO: Entry to CS");
			System.out.println("INFO: Increasing available count of mapthread to: "
					+ (MapperThreadPool.availableCount + 1));
			MapperThreadPool.availableCount++;
			// Flip completed flag in the map to true and move the entry set
			// from activeMap to completedMap
			int index = -1, requiredIndex = -1;
			MapTaskStatus mapStatus = null;
			for (MapTaskStatus mapTaskStatus : MapperThreadPool.activeThreadTask) {
				index++;
				if (mapTaskStatus.getJobId() == mapTaskInfo.getJobId()
						&& mapTaskStatus.getTaskId() == mapTaskInfo.getTaskId()) {
					System.out.println("INFO: Found the entry of completed task in ActiveTaskList");
					requiredIndex = index;
					mapStatus = mapTaskStatus;
					break;
				}
			}
			MapTaskStatus.Builder mapBuild = MapTaskStatus.newBuilder(mapStatus);
			mapBuild.setTaskCompleted(true);
			String fileName = "job_" + mapTaskInfo.getJobId() + "_map_" + mapTaskInfo.getTaskId();
			mapBuild.setMapOutputFile(fileName);
			System.out
					.println("INFO: Moving Completed Task from ActiveThreadList to completedThreadList ");
			MapperThreadPool.activeThreadTask.remove(requiredIndex);
			MapperThreadPool.completedThreadTask.add(mapBuild.build());
			System.out.println("INFO: Exit from CS");
		}
	}

}
