/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import storm.autoscale.scheduler.util.UtilFunctions;

/**
 * @author Roland
 *
 */
public class ComponentWindowedStats {

	private final String id;
	private final HashMap<Integer, Long> inputRecords;
	private final HashMap<Integer, Long> executedRecords;
	private final HashMap<Integer, Long> outputRecords;
	private final HashMap<Integer, Double> avgLatencyRecords;
	private final HashMap<Integer, Double> selectivityRecords;
	private final HashMap<Integer, ArrayList<Double>> cpuUsageRecords;
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger("ComponentWindowedStats");
	
	/**
	 * 
	 */
	public ComponentWindowedStats(String id, HashMap<Integer, Long> inputs, HashMap<Integer, Long> executed, HashMap<Integer, Long> outputs, HashMap<Integer, Double> avgLatency, HashMap<Integer, Double> selectivity, HashMap<Integer, ArrayList<Double>> cpuUsage) {
		this.id = id;
		this.inputRecords = inputs;
		this.executedRecords = executed;
		this.outputRecords = outputs;
		this.avgLatencyRecords = avgLatency;
		this.selectivityRecords = selectivity;
		this.cpuUsageRecords = cpuUsage;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	
	public static <T> ArrayList<Integer> getRecordedTimestamps(HashMap<Integer, T> records){
		ArrayList<Integer> orderedTimestamps = new ArrayList<>();
		Set<Integer> timestamps = records.keySet();
		for(Integer timestamp : timestamps){
			orderedTimestamps.add(timestamp);
		}
		Collections.sort(orderedTimestamps, new UtilFunctions.DecreasingIntOrder());
		return orderedTimestamps;
	}
	
	public static <T extends Number> Double getRecord(HashMap<Integer, T> records, Integer rank){
		ArrayList<Integer> timestamps = ComponentWindowedStats.getRecordedTimestamps(records);
		Integer timestamp = -1;
		try{
			timestamp = timestamps.get(rank);
		}catch(IndexOutOfBoundsException e){
			//logger.warning("The rank " + rank + " passed in argument does not match with any timestamp/record pair");
		}
		Double result = 0.0;
		try{
			T record = records.get(timestamp);
			result = new BigDecimal(record.toString()).doubleValue();
		}catch(NullPointerException e){
			//logger.warning("There is not record for the given timestamp.");
		}
		return result;
	}
	
	public static <T extends Number> Double getLastRecord(HashMap<Integer, T> records){
		return ComponentWindowedStats.getRecord(records, 0);
	}
	
	public static <T extends Number> Double getOldestRecord(HashMap<Integer, T> records){
		int oldestRank = ComponentWindowedStats.getRecordedTimestamps(records).size() - 1;
		return ComponentWindowedStats.getRecord(records, oldestRank);
	}


	//define a method which takes records of a dimension and computes the list of variations 
	//for example let consider records R={700, 690, 560, 500} according to decreasing order of timestamps
	//we want to get the list {10, 130, 60} which define variations
	
	public static ArrayList<Double> getVariations(HashMap<Integer, ? extends Number> records){
		ArrayList<Double> variations = new ArrayList<>();
		ArrayList<Integer> recordedTimestamps = ComponentWindowedStats.getRecordedTimestamps(records);
		for(int i = 0; i < recordedTimestamps.size() - 1; i++){
			Integer currentTimestamp = recordedTimestamps.get(i);
			Integer previousTimestamp = recordedTimestamps.get(i + 1);
			Double currentRecord = new BigDecimal(records.get(currentTimestamp).toString()).doubleValue();
			Double previousRecord = new BigDecimal(records.get(previousTimestamp).toString()).doubleValue();
			variations.add(currentRecord - previousRecord);
		}
		return variations;
	}
	
	public static boolean isSignificantSample(HashMap<? extends Number, ? extends Number> records, Integer windowSize, Integer frequency){
		Integer significant = (windowSize / frequency) - 1;// we accept that an offset, due to data storage and retrieval, prevents from getting the oldest value
		return records.size() >= significant;
	}
	
	/**
	 * @return the inputRecords
	 */
	public HashMap<Integer, Long> getInputRecords() {
		return inputRecords;
	}

	/**
	 * @return the executedRecords
	 */
	public HashMap<Integer, Long> getExecutedRecords() {
		return executedRecords;
	}

	/**
	 * @return the outputRecords
	 */
	public HashMap<Integer, Long> getOutputRecords() {
		return outputRecords;
	}

	/**
	 * @return the avgLatencyRecords
	 */
	public HashMap<Integer, Double> getAvgLatencyRecords() {
		return avgLatencyRecords;
	}

	/**
	 * @return the selectivityRecords
	 */
	public HashMap<Integer, Double> getSelectivityRecords() {
		return selectivityRecords;
	}

	/**
	 * @return the cpuUsageRecords
	 */
	public HashMap<Integer, ArrayList<Double>> getCpuUsageRecords() {
		return cpuUsageRecords;
	}
	
	public HashMap<Integer, Double> getAvgCpuUsageRecords(){
		HashMap<Integer, Double> result = new HashMap<>();
		ArrayList<Integer> timestamps = ComponentWindowedStats.getRecordedTimestamps(this.cpuUsageRecords);
		for(Integer timestamp : timestamps){
			ArrayList<Double> cpuUsages = this.cpuUsageRecords.get(timestamp);
			result.put(timestamp, UtilFunctions.getAvgValue(cpuUsages));
		}
		return result;
	}
	
	public HashMap<Integer, Double> getMaxCpuUsageRecords(){
		HashMap<Integer, Double> result = new HashMap<>();
		ArrayList<Integer> timestamps = ComponentWindowedStats.getRecordedTimestamps(this.cpuUsageRecords);
		for(Integer timestamp : timestamps){
			ArrayList<Double> cpuUsages = this.cpuUsageRecords.get(timestamp);
			result.put(timestamp, UtilFunctions.getMaxValue(cpuUsages));
		}
		return result;
	}
	
	public HashMap<Integer, Double> getMinCpuUsageRecords(){
		HashMap<Integer, Double> result = new HashMap<>();
		ArrayList<Integer> timestamps = ComponentWindowedStats.getRecordedTimestamps(this.cpuUsageRecords);
		for(Integer timestamp : timestamps){
			ArrayList<Double> cpuUsages = this.cpuUsageRecords.get(timestamp);
			result.put(timestamp, UtilFunctions.getMinValue(cpuUsages));
		}
		return result;
	}

	public Long getTotalInput(){
		Long result = 0L;
		for(Integer timestamp : this.getInputRecords().keySet()){
			result += this.getInputRecords().get(timestamp);
		}
		return result;
	}
	
	public Long getTotalExecuted(){
		Long result = 0L;
		for(Integer timestamp : this.getExecutedRecords().keySet()){
			result += this.getExecutedRecords().get(timestamp);
		}
		return result;
	}
	
	public boolean hasRecords(){
		ArrayList<Integer> inputTimestamps = ComponentWindowedStats.getRecordedTimestamps(this.inputRecords);
		ArrayList<Integer> executedTimestamps = ComponentWindowedStats.getRecordedTimestamps(this.executedRecords);
		ArrayList<Integer> outputTimestamps = ComponentWindowedStats.getRecordedTimestamps(this.outputRecords);
		ArrayList<Integer> avgLatencyTimestamps = ComponentWindowedStats.getRecordedTimestamps(this.avgLatencyRecords);
		ArrayList<Integer> selectivityTimestamps = ComponentWindowedStats.getRecordedTimestamps(this.selectivityRecords);
		ArrayList<Integer> cpuTimestamps = ComponentWindowedStats.getRecordedTimestamps(this.cpuUsageRecords);
		
		return (!inputTimestamps.isEmpty() && !executedTimestamps.isEmpty() && !outputTimestamps.isEmpty()
				&& !avgLatencyTimestamps.isEmpty() && !selectivityTimestamps.isEmpty() && !cpuTimestamps.isEmpty());
	}
	
	
	
	@Override
	public String toString(){
		String result = "Statistics of component " + this.getId() + "\n";
		HashMap<Integer, Long> input = this.getInputRecords();
		HashMap<Integer, Long> executed = this.getExecutedRecords();
		HashMap<Integer, Long> output = this.getOutputRecords();
		HashMap<Integer, Double> latency = this.getAvgLatencyRecords();
		HashMap<Integer, Double> selectivity = this.getSelectivityRecords();
		HashMap<Integer, ArrayList<Double>> cpuUsage = this.getCpuUsageRecords();
		result += "Inputs :";
		for(Integer timestamp : input.keySet()){
			result += " [" + timestamp + "->" + input.get(timestamp) + "]";
		}
		result += "\n";
		result += "Executed :";
		for(Integer timestamp : executed.keySet()){
			result += " [" + timestamp + "->" + executed.get(timestamp) + "]";
		}
		result += "\n";
		result += "Outputs :";
		for(Integer timestamp : output.keySet()){
			result += " [" + timestamp + "->" + output.get(timestamp) + "]";
		}
		result += "\n";
		result += "Latencies :";
		for(Integer timestamp : latency.keySet()){
			result += " [" + timestamp + "->" + latency.get(timestamp) + "]";
		}
		result += "\n";
		result += "Selectivities :";
		for(Integer timestamp : selectivity.keySet()){
			result += " [" + timestamp + "->" + selectivity.get(timestamp) + "]";
		}
		result += "\n";
		result += "Cpu usages :";
		for(Integer timestamp : cpuUsage.keySet()){
			result += " [" + timestamp + "->" + cpuUsage.get(timestamp) + "]";
		}
		return result;
	}
}