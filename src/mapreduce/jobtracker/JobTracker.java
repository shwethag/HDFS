package mapreduce.jobtracker;

import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

import mapreduce.MapReduce;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private static final String MAPPERS_REDUCERS = "mappers_reducers.ini";
	private static final long serialVersionUID = 1L;
	private static int jobIdCnt = 0;

	public JobTracker() throws RemoteException {
		super();
	}
	
	static{
		Scanner sc = null;
		try{
			sc = new Scanner(new File(MAPPERS_REDUCERS));
			while(sc.hasNext()){
				String line = sc.nextLine();
				String map_red[] = line.split("-");
			}
		}catch(IOException e){
			System.out.println("Error loading init files");
		}
	}
	
	@Override
	public byte[] jobSubmit(byte[] jobSubmitRequest) {
		
/*		message JobSubmitRequest {
			  optional string mapName = 1; // Java class, or name of C .so
			  optional string reducerName = 2; // Java class, or name of C .so
			  optional string inputFile = 3;
			  optional string outputFile = 4;
			  optional int32 numReduceTasks = 5;
			}
			
			message JobSubmitResponse {
			  optional int32 status = 1;
			  optional int32 jobId = 2;
			}
*/
		try {
			MapReduce.JobSubmitRequest jobSubmit = MapReduce.JobSubmitRequest.parseFrom(jobSubmitRequest);
			
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
	            System.setSecurityManager ( new RMISecurityManager() );
	     
		  try {
				final JobTracker jobTracker = new JobTracker();
				Registry registry = LocateRegistry.createRegistry(1099);
				registry.bind("JobTracker", jobTracker);
				System.out.println("Service Bound..");
				new Thread(new Runnable() {
					
					@Override
					public void run() {
						while(true){
							try {
								Thread.sleep(30000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						//	jobTracker.dumpToFile();
							
						}
						
					}
					}).start();	
				
				
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (AlreadyBoundException e) {
				e.printStackTrace();
			}   
	}

	private class MapReducePair{
		private String mapper;
		private String reducer;
		
		public MapReducePair(String map,String reduce) {
			mapper = map;
			reducer = reducer;
		}
		
	}
}
