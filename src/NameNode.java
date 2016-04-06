import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import INameNode.INameNode;

import com.google.protobuf.InvalidProtocolBufferException;

public class NameNode extends UnicastRemoteObject implements INameNode{

	private static final String COMMA = ",";
	private static final String BLOCK_ID_LIST_MAP_FILE = "blockIdListMap.dat";
	private static final String BLOCKMAPFILE = "blockMap.dat";
	private static final String EQUALS = "=";
	private static final String DATANODE_INI = "datanode.ini";
	private static final String FILEHANDLE_MAP = "filehandleMap.dat";
	private static final int FAILURE = 0;
	private static final int SUCCESS = 1;
	private static final long serialVersionUID = 1L;
	private HashMap<String, Integer> fileHandlerMap;
	private HashMap<Integer, Boolean> activeHandleMode;
	private HashMap<Integer,List<Integer> > blockMap;
	public static Integer  fileHandleCounter = 1,blockIdCounter=1;
	private static final Object assignLock = new Object();
	private static final Object openLock = new Object();
	private HashMap<Integer, String> idLocMap;
	private HashMap<Integer,Set<Integer> > blockIdListMap;
	private int dataNodeCount =0;
	protected NameNode() throws RemoteException {
		super();
		fileHandlerMap = new HashMap<String, Integer>();
		activeHandleMode = new HashMap<Integer, Boolean>(); // Here value False - Write mode True - Read Mode
		blockMap = new HashMap<>();
		idLocMap = new HashMap<>();
		blockIdListMap = new HashMap<>();
		init();
	}
	
	private void init(){
		
		// To Load datanode locations and ID to idLocMap
		Scanner sc = null;
		
		try {
			sc = new Scanner(new File(DATANODE_INI));
			while(sc.hasNext()){
				String line[]= sc.nextLine().split(EQUALS);
				idLocMap.put(Integer.parseInt(line[0]), line[1]);
				dataNodeCount++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(sc!=null)
				sc.close();
		}
		loadMaps();
						
	}
	
	public synchronized void loadMaps(){
		Scanner sc = null;
		System.out.println("INFO: Loading maps back to in-memory");
		try {
			sc = new Scanner(new File(BLOCKMAPFILE));
			while(sc.hasNext()){
				String line=sc.nextLine();
				if(line.length()==0)
					break;
				String val[] = line.split(EQUALS);
				int key=Integer.parseInt(val[0]);
				System.out.println(line);
				String values[] = val[1].split(COMMA);
				List<Integer> blockNums = new ArrayList<>();
				for(int i=0;i<values.length;i++){
					blockNums.add(Integer.parseInt(values[i]));
				}
				blockMap.put(key, blockNums);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(sc!=null)
				sc.close();
		}
		
		System.out.println("INFO: To Load Block List Map");
		
		try {
			sc = new Scanner(new File(BLOCK_ID_LIST_MAP_FILE));
			while(sc.hasNext()){
				String line=sc.nextLine();
				if(line.length()==0)
					break;
				String val[] = line.split(EQUALS);
				int key=Integer.parseInt(val[0]);
				String values[] = val[1].split(COMMA);
				Set<Integer> blockNums = new HashSet<>();
				for(int i=0;i<values.length;i++){
					blockNums.add(Integer.parseInt(values[i]));
				}
				blockIdListMap.put(key, blockNums);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(sc!=null)
				sc.close();
		}
		
		System.out.println("INFO: To Load Filehandle Map");
		
		try {
			sc = new Scanner(new File(FILEHANDLE_MAP));
			while(sc.hasNext()){
				String line=sc.nextLine();
				if(line.length()==0)
					break;
				String val[] = line.split(EQUALS);
				String fname=val[0];
				int handle=Integer.parseInt(val[1]);
				fileHandlerMap.put(fname, handle);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(sc!=null)
				sc.close();
		}
		
	}
	
	public synchronized void dumpToFile(){
		PrintWriter pr = null;
		System.out.println("INFO: Dumping BlockMap to file");
		try {
			pr = new PrintWriter(new File(BLOCKMAPFILE));
			for (Map.Entry<Integer, List<Integer> > entry : blockMap.entrySet()) {
				System.out.print(entry.getKey().toString()+EQUALS);
				pr.print(entry.getKey().toString());
				pr.print(EQUALS);
				for (Integer blockNum : entry.getValue()) {
					System.out.print(blockNum.toString()+COMMA);
					pr.print(blockNum.toString());
					pr.print(COMMA);
				}
				System.out.println();
				pr.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(pr!=null)
				pr.close();
		}
		
		System.out.println("INFO: Dumping blockIdListMap to file");
		try {
			pr = new PrintWriter(new File(BLOCK_ID_LIST_MAP_FILE));
			for (Map.Entry<Integer, Set<Integer> > entry : blockIdListMap.entrySet()) {
				pr.print(entry.getKey().toString());
				pr.print(EQUALS);
				for (Integer blockNum : entry.getValue()) {
					pr.print(blockNum.toString());
					pr.print(COMMA);
				}
				pr.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(pr!=null)
				pr.close();
		}
		
		
		System.out.println("INFO: Dumping FILEHANDLEMAP to file");
		try {
			pr = new PrintWriter(new File(FILEHANDLE_MAP));
			for (Map.Entry<String, Integer > entry : fileHandlerMap.entrySet()) {
				pr.print(entry.getKey());
				pr.print(EQUALS);
				pr.print(entry.getValue().toString());
				pr.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(pr!=null)
				pr.close();
		}
	}

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		// Check if file has been opened by any other client by checking in opened-File Set. If not return true and a unique handle else return false
		Hdfs.OpenFileResponse.Builder openFileResponseBuilder = Hdfs.OpenFileResponse.newBuilder();
		try {
			Hdfs.OpenFileRequest openFileRequest =  Hdfs.OpenFileRequest.parseFrom(inp);
			
			
			if(!fileHandlerMap.containsKey(openFileRequest.getFileName())){
				System.out.println("INFO: File is new and requesting for new handle");
				if(openFileRequest.getForRead()==true){
					System.out.println("ERROR: Wrong access for new file");
					openFileResponseBuilder.setStatus(FAILURE);
				}
				else{
					System.out.println("INFO: new file to put in handle " + openFileRequest.getFileName());
					synchronized (openLock) {
						fileHandlerMap.put(openFileRequest.getFileName(), fileHandleCounter);
						activeHandleMode.put(fileHandleCounter, openFileRequest.getForRead());
						openFileResponseBuilder.setHandle(fileHandleCounter);
						openFileResponseBuilder.setStatus(SUCCESS);
						fileHandleCounter++;
						
					}
				}
			}else{
				Integer fileHandle = fileHandlerMap.get(openFileRequest.getFileName());
				//File handle has been  created but no client is using
				if(!activeHandleMode.containsKey(fileHandle)){
					activeHandleMode.put(fileHandle,openFileRequest.getForRead());
					openFileResponseBuilder.setStatus(SUCCESS);
					openFileResponseBuilder.setHandle(fileHandle);
				}//File active but read read situation
				else if(openFileRequest.getForRead() == true && 
						activeHandleMode.get(fileHandle)==true ){
						openFileResponseBuilder.setStatus(SUCCESS);
						openFileResponseBuilder.setHandle(fileHandle);
				}else{
					//error condition
					System.out.println("ERROR: File already opened in read mode by other users");
					openFileResponseBuilder.setStatus(FAILURE);
				}
				if(openFileResponseBuilder.getStatus()==SUCCESS){
					//Read mode Hence need to add all block numbers
					System.out.println("INFO: Adding BlockNumbers for Open call for file: "+openFileRequest.getFileName());
					for (int blockNum: blockMap.get(fileHandle)) {
						System.out.println("blk: " + blockNum);
						openFileResponseBuilder.addBlockNums(blockNum);
					}
				}
			}
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return openFileResponseBuilder.build().toByteArray();
	}
	
	public List<String> getRandomDataNodeIp(){
		List<String> dataNodeIp = new ArrayList<String>();
		
		Random rand = new Random();
		while(dataNodeIp.size()<2){
			int randId = rand.nextInt(dataNodeCount)+1;
			if(!dataNodeIp.contains(idLocMap.get(randId))){
				dataNodeIp.add(idLocMap.get(randId));
			}
		}
		return dataNodeIp;
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		Hdfs.CloseFileResponse.Builder closeFileresponsebuilder = Hdfs.CloseFileResponse.newBuilder();
		
		try {
			Hdfs.CloseFileRequest closeFileRequest = Hdfs.CloseFileRequest.parseFrom(inp);
			synchronized (openLock) {
				if(activeHandleMode.containsKey(closeFileRequest.getHandle())){
					activeHandleMode.remove(closeFileRequest.getHandle());
					closeFileresponsebuilder.setStatus(SUCCESS);
				}else{
					closeFileresponsebuilder.setStatus(FAILURE);
				}
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		
		return closeFileresponsebuilder.build().toByteArray();
	}

	Hdfs.BlockLocations getBlockLocationOfDataNodes(int blockId){
		System.out.println("INFO: getBlockLocationOfDataNodes"+ blockId);
		Hdfs.BlockLocations.Builder blockLocBuilder = Hdfs.BlockLocations.newBuilder();
		blockLocBuilder.setBlockNumber(blockId);
		List<String> dataIpList = getRandomDataNodeIp();
		String dataport="1099";
		for (String datanodeIp : dataIpList) {
			System.out.println("IP: "+datanodeIp);
			Hdfs.DataNodeLocation.Builder dataNodeLocBuilder = Hdfs.DataNodeLocation.newBuilder();
			dataNodeLocBuilder.setIp(datanodeIp);
			dataNodeLocBuilder.setPort(Integer.parseInt(dataport));
			blockLocBuilder.addLocations(dataNodeLocBuilder.build());
		}
		
		return blockLocBuilder.build();
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		Hdfs.AssignBlockResponse.Builder assignBlockResBuilder =  Hdfs.AssignBlockResponse.newBuilder();
		
		try {
			System.out.println("Assign Block() started");
			Hdfs.AssignBlockRequest assignBlockRequest = Hdfs.AssignBlockRequest.parseFrom(inp);
			int handle = assignBlockRequest.getHandle();
			synchronized(assignLock){
				System.out.println("Entered Sync Lock Assign");
				if(!activeHandleMode.containsKey(handle)){
					assignBlockResBuilder.setStatus(FAILURE);
				}else{
					if(!blockMap.containsKey(handle)){
						//new one
						List<Integer> blockIdList = new ArrayList<>();
						blockIdList.add(blockIdCounter);
						blockMap.put(handle, blockIdList);
						
					}else{
						//append 
						List<Integer> blockIdList = blockMap.get(handle);
						blockIdList.add(blockIdCounter);
						blockMap.put(handle, blockIdList);
						
					}
					assignBlockResBuilder.setStatus(SUCCESS);
					//set data Node Lockt
					System.out.println("Calling getblock loc()");
					assignBlockResBuilder.setNewBlock(getBlockLocationOfDataNodes(blockIdCounter));
					blockIdCounter++;
				}
			}
			
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return assignBlockResBuilder.build().toByteArray();
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		Hdfs.ListFilesResponse.Builder listFileResBuilder = Hdfs.ListFilesResponse.newBuilder();
		
		for (String file : fileHandlerMap.keySet()) {
			listFileResBuilder.addFileNames(file);
		}
		listFileResBuilder.setStatus(SUCCESS);
		
		return listFileResBuilder.build().toByteArray();
	}
	
	private Hdfs.BlockLocations getBlockLocation(int blkNum){
		System.out.println("INFO: Get block location request received for block " + blkNum);
		Hdfs.BlockLocations.Builder blockLoc = Hdfs.BlockLocations.newBuilder();
		if(!blockIdListMap.containsKey(blkNum)){
			System.out.println("ERROR: Block number not present");
			return null;
		}
		blockLoc.setBlockNumber(blkNum);
		for (int locId : blockIdListMap.get(blkNum)) {
			Hdfs.DataNodeLocation.Builder dataLoc = Hdfs.DataNodeLocation.newBuilder();
			dataLoc.setIp(idLocMap.get(locId));
			blockLoc.addLocations(dataLoc);
		}
		return blockLoc.build();
	}
	
	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		System.out.println("INFO: Get block locations request received");
		Hdfs.BlockLocationResponse.Builder blockLocResBuilder = Hdfs.BlockLocationResponse.newBuilder();
		
		try {
			Hdfs.BlockLocationRequest blockLocationRequest = Hdfs.BlockLocationRequest.parseFrom(inp);
			for (int blkNum : blockLocationRequest.getBlockNumsList()) {
				Hdfs.BlockLocations blockLocations = getBlockLocation(blkNum);
				if(blockLocations == null){
					System.out.println("Error: Unable to retrieve block location for "+ blkNum);
					blockLocResBuilder.setStatus(FAILURE);
					return blockLocResBuilder.build().toByteArray();
				}
				
				blockLocResBuilder.addBlockLocations(blockLocations);
			}
			blockLocResBuilder.setStatus(SUCCESS);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		
		return blockLocResBuilder.build().toByteArray();
	}
	

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		System.out.println("INFO: block report received");
		Hdfs.BlockReportResponse.Builder blkreportres = Hdfs.BlockReportResponse.newBuilder();
		
		try {
			Hdfs.BlockReportRequest blockReportRequest = Hdfs.BlockReportRequest.parseFrom(inp);
			int id = blockReportRequest.getId();
			for (int blkNum : blockReportRequest.getBlockNumbersList()) {
				if(!blockIdListMap.containsKey(blkNum)){
					Set<Integer> idList = new HashSet<>();
					blockIdListMap.put(blkNum, idList);
				}
					
				Set<Integer> idList = blockIdListMap.get(blkNum);
				idList.add(id);
				blockIdListMap.put(blkNum,idList);
				blkreportres.addStatus(SUCCESS);
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return blkreportres.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		Hdfs.HeartBeatResponse.Builder hb = Hdfs.HeartBeatResponse.newBuilder();
		hb.setStatus(SUCCESS);
		return hb.build().toByteArray();
	}
	
	public static void main(String[] args) {
		 if (System.getSecurityManager() == null)
	            System.setSecurityManager ( new RMISecurityManager() );
	        
	        System.out.println("Run properly");
	        
	     try {
			final NameNode namenode = new NameNode();
			Naming.bind("NameNode", namenode);
			
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					while(true){
						try {
							Thread.sleep(30000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						namenode.dumpToFile();
						
					}
					
				}
				}).start();	
			
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}   
	}

}
