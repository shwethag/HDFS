package mapreduce.jobtracker;

public class Job {

	private int jobId;
	private String inputFileName;
	private String outputFileName;
	private int reducersCnt;
	
	public Job(int jobId, String inputFileName, String outputFileName, int reducersCnt) {
		super();
		this.jobId = jobId;
		this.inputFileName = inputFileName;
		this.outputFileName = outputFileName;
		this.reducersCnt = reducersCnt;
	}

	public int getJobId() {
		return jobId;
	}

	public String getInputFileName() {
		return inputFileName;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public int getReducersCnt() {
		return reducersCnt;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + jobId;
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
		Job other = (Job) obj;
		if (jobId != other.jobId)
			return false;
		return true;
	}
	
	
	
}
