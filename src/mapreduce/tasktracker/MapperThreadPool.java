package mapreduce.tasktracker;

import java.util.HashMap;
import java.util.Map;

import mapreduce.MapReduce.MapTaskInfo;
import mapreduce.MapReduce.MapTaskStatus;

public class MapperThreadPool {
	public static int occupiedCount = 0;
	public static int maxThreadCount = 3;
	public static int availableCount = maxThreadCount;
	public static Object lock = new Object();
	public Map<String,MapTaskStatus> activeThreadTask = null;
	
	public MapperThreadPool(){
		System.out.println("INFO: Mapper Thread Constructor Started");
		activeThreadTask = new HashMap<>();
		
	}
	
	public void addNewMapTask(MapTaskInfo mapTask){
		
	}
	
	public void addNewReduceTask(){
		
	}
}
