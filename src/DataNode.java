import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import IDataNode.IDataNode;
import INameNode.INameNode;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class DataNode extends UnicastRemoteObject implements IDataNode {

	private static final int PORT = 1099;
	private static final String COMMA = ",";
	private static final String DEL = "/";
	private static final String DAT = ".dat";
	private static final String CONFIG = "config.ini";
	private static final String BLOCK = "blocksize";
	private static final long serialVersionUID = 1L;
	private static final int FAILURE = 0;
	private static final int SUCCESS = 1;
	private static final String NAMENODE = "namenode";
	private static final String DATA_FOLDER = "./data";
	private static final String BLOCK_LIST_FILE = "blockList.dat";
	private int dataNode_ID;
	private List<Integer> blockIdList;
	private INameNode namenode;
	private String namenodeIp;
	
	public DataNode(int id) throws RemoteException {
		super();
		dataNode_ID = id;
		namenode = null;
		String data[];
		blockIdList = new ArrayList<Integer>();
		Scanner sc = null;
		try {
			sc = new Scanner(new File(CONFIG));
			while (sc.hasNext()) {
				data = sc.nextLine().split("=");
				if (NAMENODE.equals(data[0])) {
					namenodeIp = data[1];
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (sc != null)
				sc.close();
		}
		loadMaps();
		connectNameNode();
	}
	
	public synchronized void loadMaps(){
		Scanner sc = null;
		System.out.println("INFO: Loading maps back to in-memory");
		try {
			sc = new Scanner(new File(BLOCK_LIST_FILE));
			while(sc.hasNext()){
				String line=sc.nextLine();
				String blocks[] = line.split(COMMA);
				blockIdList.clear();
				for(String blkStr:blocks){
					blockIdList.add(Integer.parseInt(blkStr));
				}
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
			pr = new PrintWriter(new File(BLOCK_LIST_FILE));
			for (int blk : blockIdList) {
				pr.print(blk);
				pr.print(',');
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally{
			if(pr!=null)
				pr.close();
		}
		
	}

	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException {
		System.out.println("INFO: Received read block request ");
		Hdfs.ReadBlockResponse.Builder readBlkResponse = Hdfs.ReadBlockResponse.newBuilder();
		try {
			Hdfs.ReadBlockRequest readBlkReq = Hdfs.ReadBlockRequest.parseFrom(inp);
			int blkNum = readBlkReq.getBlockNumber();
			if(!blockIdList.contains(blkNum)){
				System.out.println("ERROR: Block not present here.." + blkNum);
				readBlkResponse.setStatus(FAILURE);
			}else{
				BufferedReader reader = null;
				try{
					reader = new BufferedReader(new FileReader(new File(DATA_FOLDER+DEL+blkNum+DAT)));
					StringBuilder builder = new StringBuilder();
					int c;
					while((c= reader.read())!=-1){
						builder.append((char)c);
					}
					readBlkResponse.setStatus(SUCCESS);
					readBlkResponse.addData(ByteString.copyFrom(builder.toString().getBytes()));
				}catch(IOException e){
					System.out.println("ERROR: unable to read block");
					readBlkResponse.setStatus(FAILURE);
				} finally{
					if(reader!=null)
						try {
							reader.close();
						} catch (IOException e) {
							System.out.println("WARN: Error closing file");
							e.printStackTrace();
						}
					
				}
			}
		} catch (InvalidProtocolBufferException e) {
			System.out.println("ERROR: Failed to process read block request");
			readBlkResponse.setStatus(FAILURE);
			e.printStackTrace();
		}
		
		return readBlkResponse.build().toByteArray();
		
	}
	
	private void connectNameNode() {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			Registry registry = LocateRegistry.getRegistry(namenodeIp);
			namenode = (INameNode) registry.lookup("NameNode");
		} catch ( RemoteException | NotBoundException e) {
			System.out.println("ERROR: Error in PUT request...Returning......");
			e.printStackTrace();
		}
	}

	
	public void sendBlockReport(){
		System.out.println("INFO: Sending block report with id"+ dataNode_ID);
		Hdfs.BlockReportRequest.Builder blkReportReq = Hdfs.BlockReportRequest.newBuilder();
		blkReportReq.setId(dataNode_ID);
		for (Integer blkId : blockIdList) {
			System.out.println("ind loop");
			System.out.println("gh:" + blkId +' ' + blkId + ':' );
			blkReportReq.addBlockNumbers(blkId);
		}
		System.out.println();
		if (namenode == null) {
			System.out.println("ERROR: Unable to send Block report Name node is not connected ");
			return;
		}
		
		try {
			namenode.blockReport(blkReportReq.build().toByteArray());
		} catch (RemoteException e) {
			System.out.println("ERROR: Failed to  send Block report ");
			e.printStackTrace();
		}
		
	}

	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		System.out.println("INFO: Received write block request");
		FileOutputStream writer = null;
		Hdfs.WriteBlockResponse.Builder writeResBuilder  = Hdfs.WriteBlockResponse.newBuilder();
		try {
			Hdfs.WriteBlockRequest writeRequest = Hdfs.WriteBlockRequest.parseFrom(inp);
			byte[] data = writeRequest.getData(0).toByteArray();
			Hdfs.BlockLocations blocklocations = writeRequest.getBlockInfo();
			int blockNum = blocklocations.getBlockNumber();
			File block = new File(DATA_FOLDER+DEL+blockNum+DAT);
			writer = new FileOutputStream(block);
			writer.write(data);
			
			if(!forwardWrite(blocklocations,writeRequest.getData(0))){
				System.out.println("ERROR: Failed to send forward request");
				writeResBuilder.setStatus(FAILURE);
			}else{
				System.out.println("INFO: Added block id " + blockNum);
				writeResBuilder.setStatus(SUCCESS);
				blockIdList.add(blockNum);
				sendBlockReport();
				dumpToFile();
			}
		} catch (IOException e) {
			System.out.println("ERROR: write block error");
			writeResBuilder.setStatus(FAILURE);
			e.printStackTrace();
		} finally{
			if(writer!=null) {
				try {
					writer.close();
				} catch (IOException e) {
					writeResBuilder.setStatus(FAILURE);
					System.out.println("ERROR: IOException in close");
					e.printStackTrace();
				}
			}
		}
		
		return writeResBuilder.build().toByteArray();
	}
	
	private IDataNode connectDatanode(Hdfs.DataNodeLocation datanodeAddress) {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			// TODO: Check for port usage
			Registry registry = LocateRegistry.getRegistry(datanodeAddress.getIp());
			IDataNode datanode = (IDataNode) registry.lookup("DataNode");
			return datanode;
		} catch ( RemoteException | NotBoundException e) {
			System.out
					.println("ERROR: Error in connect datanode request...Returning......");
			e.printStackTrace();
			return null;
		}

	}
	
	private byte[] constructWrite(ByteString data, Hdfs.BlockLocations locations) {
		Hdfs.WriteBlockRequest.Builder writeBlockBuilder = Hdfs.WriteBlockRequest.newBuilder();
		writeBlockBuilder.setBlockInfo(locations);
		writeBlockBuilder.addData(data);
		return writeBlockBuilder.build().toByteArray();
	}

	private boolean forwardWrite(Hdfs.BlockLocations blocklocations, ByteString data) throws RemoteException, InvalidProtocolBufferException {
		if(blocklocations.getLocationsCount() == 0){
			System.out.println("INFO: No other nodes to forward");
			return true;
		}
		Hdfs.DataNodeLocation datanodelocation = blocklocations.getLocations(0);
		IDataNode datanode = connectDatanode(datanodelocation);
		if(datanode==null){
			System.out.println("ERROR: Could not connect to data node");
			return false;
		}
		Hdfs.BlockLocations.Builder newBlockLocationsBuilder = Hdfs.BlockLocations.newBuilder();
		newBlockLocationsBuilder.setBlockNumber(blocklocations.getBlockNumber());
		for (int i = 1; i < blocklocations.getLocationsCount(); i++) {
			newBlockLocationsBuilder.addLocations(blocklocations.getLocations(i));
		}
		byte[] writeReq = constructWrite(data, newBlockLocationsBuilder.build());
		byte[] writeReponseArray = datanode.writeBlock(writeReq);
		Hdfs.WriteBlockResponse writeResponse = Hdfs.WriteBlockResponse
				.parseFrom(writeReponseArray);
		if (writeResponse.getStatus() == FAILURE) {
			System.out.println("ERROR: Failed to write block in forward request");
			return false;
		}
		return true;
	}
	
	public void sendHeartbeat(){
		Hdfs.HeartBeatRequest.Builder heartbeat = Hdfs.HeartBeatRequest.newBuilder();
		heartbeat.setId(dataNode_ID);
		System.out.println("Sending Heart Beat :"+ dataNode_ID);
		try {
			byte[] res=namenode.heartBeat(heartbeat.build().toByteArray());
			Hdfs.HeartBeatResponse heartBeatResponse = Hdfs.HeartBeatResponse.parseFrom(res);
			if(heartBeatResponse.getStatus()==SUCCESS){
				System.out.println("Heartbeat Response Successfull");
			}else{
				System.out.println("Hearbeat response Failed");
			}
		} catch (RemoteException | InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (System.getSecurityManager() == null)
			System.setSecurityManager(new RMISecurityManager());

		try {
			final DataNode datanode = new DataNode(Integer.parseInt(args[0]));
			Registry registry = null;
			try{
				registry = LocateRegistry.createRegistry(PORT);
			}
			catch(ExportException e){
				System.out.println("Registry already created.. getting the registry");
				registry = LocateRegistry.getRegistry(PORT);
			}
			 registry.bind("DataNode", datanode);
			System.out.println("Service Bound...");
			 
		    new Thread(new Runnable() {
				
				@Override
				public void run() {
					while(true){
						try {
							Thread.sleep(30000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						datanode.sendHeartbeat();
						datanode.sendBlockReport();
						datanode.dumpToFile();
						
					}
					
				}
			}).start();	
			
		} catch (RemoteException e) {
			e.printStackTrace();
		}catch (AlreadyBoundException e) {
			e.printStackTrace();
		}

	}

}
