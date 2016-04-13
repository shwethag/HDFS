package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import IDataNode.IDataNode;
import INameNode.INameNode;
import hdfs.Hdfs;

public class Connector {

	private static final String CONFIG = "./config/config.ini";
	private static final String BLOCK = "blocksize";
	private static final String NAMENODE = "namenode";
	private static final String DATA_DIR = "./remoteData";
	private static final String DEL = "/";
	private static final String DAT = ".dat";
	private Map<String, Integer> fileHandleMap;
	
	private static final int SUCCESS = 1;
	private static final int FAILURE = 0;
	
	public int blockSize;
	private String namenodeIp;
	public INameNode namenode;
	private static Connector connector = null;
	private static Object lock = new Object();
	private Connector() {
		Scanner sc = null;
		String data[];
		namenode = null;
		try {
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
		connectNameNode();
		fileHandleMap = new HashMap<>();
	}
	
	public static Connector getConnector(){
		synchronized (lock) {
			if(connector==null){
				connector = new Connector();
				
			}
		}
		return connector;
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
	
	public IDataNode connectDatanode(Hdfs.DataNodeLocation datanodeAddress) {
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
	
	
	
	
	private byte[] constructOpen(String fileName, boolean isRead) {
		Hdfs.OpenFileRequest.Builder openBuilder = Hdfs.OpenFileRequest.newBuilder();
		openBuilder.setFileName(fileName);
		openBuilder.setForRead(isRead);
		return openBuilder.build().toByteArray();
	}
	
	public byte[] constructWrite(ByteString data, Hdfs.BlockLocations locations) {
		Hdfs.WriteBlockRequest.Builder writeBlockBuilder = Hdfs.WriteBlockRequest.newBuilder();
		writeBlockBuilder.setBlockInfo(locations);
		writeBlockBuilder.addData(data);
		return writeBlockBuilder.build().toByteArray();
	}

	public Hdfs.OpenFileResponse open(String fileName, boolean forRead) {
		if (connector.namenode == null) {
			System.out.println("Name node is not connected ");
			return null;
		}
		byte[] openReq = connector.constructOpen(fileName, forRead);
		byte[] responseArray;
		try {
			responseArray = connector.namenode.openFile(openReq);
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
	
	private byte[] buildAssignBlock(int fileHandle) {
		Hdfs.AssignBlockRequest.Builder assignBuilder = Hdfs.AssignBlockRequest.newBuilder();
		assignBuilder.setHandle(fileHandle);
		return assignBuilder.build().toByteArray();
	}
	
	public void get(String fileName) {
		Hdfs.OpenFileResponse openResponse;
		if (connector.namenode == null) {
			System.out.println("ERROR: Name node is not connected ");
			return;
		}
		openResponse = connector.open(fileName, true);
		
		if (openResponse.getStatus() == FAILURE) {
			System.out.println("ERROR: Open failed..");
			return;
		}
		List<Integer> blockNumbers = openResponse.getBlockNumsList();
		
	
		int handle = openResponse.getHandle();
		System.out.println("INFO: file handle is " + handle);
		Hdfs.BlockLocationRequest.Builder blockLocReqBuilder = Hdfs.BlockLocationRequest
				.newBuilder();
		System.out.println("INFO: Block numbers...");
		for (int blockNum : blockNumbers) {
			System.out.println("blknum:"+blockNum + ' ');
			blockLocReqBuilder.addBlockNums(blockNum);
		}
		System.out.println();
		
		FileOutputStream writer = null;
		try {
			byte[] blockLocResponseArray = connector.namenode.getBlockLocations(blockLocReqBuilder.build()
					.toByteArray());
			Map<Integer, ByteString> blockDataMap = fetchBlocks(blockLocResponseArray);

			File block = new File(connector.DATA_DIR + connector.DEL + fileName);
			writer = new FileOutputStream(block);

			for (int blk : blockNumbers) {
				if (!blockDataMap.containsKey(blk)) {
					System.out.println("ERROR: Blocks not read properly..Missing block " + blk);
					return;
				}
				ByteString blockByte = blockDataMap.get(blk);
				//System.out.println("Final dump: "+ blockByte.toStringUtf8());
				//System.out.println("Final dump: "+ blockByte.toByteArray());
				if(blockByte.isValidUtf8()){
					System.out.println("Coming inside valid UTF8 if");
					String s = blockByte.toStringUtf8();
					writer.write(s.getBytes());
				}
				else
					blockByte.writeTo(writer);
				
				//System.out.print(blockDataMap.get(blk));
			}
			Hdfs.CloseFileRequest.Builder closeBuilder = Hdfs.CloseFileRequest.newBuilder();
			closeBuilder.setHandle(handle);
			byte[] closeResponseArray = connector.namenode.closeFile(closeBuilder.build().toByteArray());
			Hdfs.CloseFileResponse closeResponse = Hdfs.CloseFileResponse
					.parseFrom(closeResponseArray);
			if (closeResponse.getStatus() == FAILURE) {
				System.out.println("ERROR: Error in close request for file " + fileName);

			}
		} catch (IOException e) {
			System.out.println("ERROR: GetBlockLocations failed..");
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					System.out.println("ERROR: failed to close file in get");
					e.printStackTrace();
				}
			}
		}

	}

	private Map<Integer, ByteString> fetchBlocks(byte[] blockLocResponseArray)
			throws InvalidProtocolBufferException {
		Hdfs.BlockLocationResponse blockLocResp = Hdfs.BlockLocationResponse
				.parseFrom(blockLocResponseArray);
		List<Hdfs.BlockLocations> blockLocations = blockLocResp.getBlockLocationsList();
		Map<Integer, ByteString> blockDataMap = new HashMap<Integer, ByteString>(); // blkid, data
		for (Hdfs.BlockLocations location : blockLocations) {
			int blockNum = location.getBlockNumber();
			for (Hdfs.DataNodeLocation datalocation : location.getLocationsList()) {
				IDataNode dataNode = connector.connectDatanode(datalocation);
				if (dataNode == null) {
					System.out.println("WARN: Couldn't connect to data node");
					continue;
				}
				Hdfs.ReadBlockRequest.Builder readBlockReq = Hdfs.ReadBlockRequest.newBuilder();
				readBlockReq.setBlockNumber(blockNum);
				try {
					byte[] readBlockRespArray = dataNode.readBlock(readBlockReq.build()
							.toByteArray());
					Hdfs.ReadBlockResponse readBlockResp = Hdfs.ReadBlockResponse
							.parseFrom(readBlockRespArray);
					if (readBlockResp.getStatus() == FAILURE) {
						System.out.println("WARN: Failed to read from datanode");
						continue;
					}
					ByteString s= null;
					for (ByteString content : readBlockResp.getDataList()) {
						s = content;
						break;
						//TODO: Check the proper get methods
					}
					//System.out.println("In Client : " + s.toStringUtf8());
					System.out.println("INFO: Read block successfully.. " + blockNum);
					blockDataMap.put(blockNum, s);
					break;

				} catch (RemoteException e) {
					System.out.println("ERROR: Read block failed..");
					e.printStackTrace();
				}

			}
		}
		return blockDataMap;
	}

	public void put(String fileName) {
		System.out.println("INFO: Received put request");
		// open
		Hdfs.OpenFileResponse openResponse;
		
		if ((openResponse=connector.open(fileName, false)) == null) {
			System.out.println("ERROR: open request failed..");
			return;
		}
		connector.fileHandleMap.put(fileName, openResponse.getHandle());
		FileInputStream fin = null;
		try {
			
			fin = new FileInputStream(new File(fileName));
			// loop
			byte [] block = new byte[connector.blockSize];
			byte [] copyBlock;
			// assign
			int c;
			while ((c = fin.read(block)) != -1) {	
				copyBlock = block;
				if(c < connector.blockSize){
					copyBlock = Arrays.copyOf(block, c);
				}
				
				if (!assignBlocks(fileName, copyBlock)) {
					System.out.println("ERROR: Failed in assigning blocks");
					return;
				}
			}
			Hdfs.CloseFileRequest.Builder closeBuilder = Hdfs.CloseFileRequest.newBuilder();
			closeBuilder.setHandle(fileHandleMap.get(fileName));
			byte[] closeResponseArray = connector.namenode.closeFile(closeBuilder.build().toByteArray());
			Hdfs.CloseFileResponse closeResponse = Hdfs.CloseFileResponse
					.parseFrom(closeResponseArray);
			if (closeResponse.getStatus() == FAILURE) {
				System.out.println("ERROR: Error in close request for file " + fileName);
				return;
			}
			fileHandleMap.remove(fileName);
			// close

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fin != null)
				try {
					fin.close();
				} catch (IOException e) {
					System.out.println("ERROR: Error closing file");
					e.printStackTrace();
				}
		}

	}

	private boolean assignBlocks(String fileName, byte [] bytes) throws RemoteException,
			InvalidProtocolBufferException {
		System.out.println("INFO: Getting blocks to write..");
		byte[] assignBlockByte = buildAssignBlock(fileHandleMap.get(fileName));
		byte[] assignBlockResponseArray = connector.namenode.assignBlock(assignBlockByte);
		System.out.println("Assign block response recieved");
		Hdfs.AssignBlockResponse assignBlockResponse = Hdfs.AssignBlockResponse
				.parseFrom(assignBlockResponseArray);
		if (assignBlockResponse.getStatus() == FAILURE) {
			System.out.println("ERROR:Block allocation failed..");
			return false;
		}
		Hdfs.BlockLocations blockLocations = assignBlockResponse.getNewBlock();
		if (blockLocations.getLocationsCount() == 0) {
			System.out.println("INFO:No data node to write...");
			return false;
		}
		Hdfs.DataNodeLocation datanodelocation = blockLocations.getLocations(0);
		System.out.println("Before connect?:");
		IDataNode datanode = connector.connectDatanode(datanodelocation);
		if (datanode == null) {
			System.out.println("ERROR: Failed to connect to data node");
			return false;
		}
		System.out.println("Connection success");

		Hdfs.BlockLocations.Builder newBlockLocationsBuilder = Hdfs.BlockLocations.newBuilder();
		newBlockLocationsBuilder.setBlockNumber(blockLocations.getBlockNumber());
		for (int i = 1; i < blockLocations.getLocationsCount(); i++) {
			newBlockLocationsBuilder.addLocations(blockLocations.getLocations(i));
		}
		ByteString byteString = ByteString.copyFrom(bytes);
		//System.out.println(byteString.toStringUtf8());
		byte[] writeRequestArray = connector.constructWrite(byteString, newBlockLocationsBuilder.build());
		System.out.println("sending writeblock request");
		byte[] writeReponseArray = datanode.writeBlock(writeRequestArray);
		Hdfs.WriteBlockResponse writeResponse = Hdfs.WriteBlockResponse
				.parseFrom(writeReponseArray);
		System.out.println("Recieved write block response");
		if (writeResponse.getStatus() == FAILURE) {
			System.out.println("ERROR: Failed to write block");
			return false;
		}
		return true;
	}

	public void list() {
		if (connector.namenode == null) {
			System.out.println("ERROR: Name node is not connected ");
			return;
		}
		Hdfs.ListFilesRequest.Builder listRequestBuilder = Hdfs.ListFilesRequest.newBuilder();
		byte[] listRequest = listRequestBuilder.build().toByteArray();
		try {
			byte[] listResponseArray = connector.namenode.list(listRequest);
			Hdfs.ListFilesResponse listResponse = Hdfs.ListFilesResponse
					.parseFrom(listResponseArray);
			if (listResponse.getStatus() == FAILURE) {
				System.out.println("ERROR: status error in list request");
				return;
			}
			System.out.println("******Files in HDFS******");
			for (String filename : listResponse.getFileNamesList()) {
				System.out.println(filename);
			}
		} catch (RemoteException | InvalidProtocolBufferException e) {
			System.out.println("ERROR: Failed in list request");
			e.printStackTrace();
		}

	}

	
}
