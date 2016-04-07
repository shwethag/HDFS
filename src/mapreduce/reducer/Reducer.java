package mapreduce.reducer;

public class Reducer implements IReducer {

	public Reducer() {
	}

	@Override
	public String reduce(String line) {
		return line;
	}

}
