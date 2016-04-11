package mapreduce.jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IJobTracker extends Remote{
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] jobSubmitRequest) throws RemoteException;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] jobStatusRequest) throws RemoteException;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] heartbeatRequest) throws RemoteException;
}
