/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import storm.autoscale.scheduler.UtilFunctions;

/**
 * @author Roland
 *
 */
public class ComponentWindowedStats {

	private final String id;
	private final HashMap<Integer, Long> inputRecords;
	private final HashMap<Integer, Long> executedRecords;
	private final HashMap<Integer, Long> outputsRecords;
	private final HashMap<Integer, Double> avgLatencyRecords;
	private final HashMap<Integer, Double> selectivityRecords;
	
	/**
	 * 
	 */
	public ComponentWindowedStats(String id, HashMap<Integer, Long> inputs, HashMap<Integer, Long> executed, HashMap<Integer, Long> outputs, HashMap<Integer, Double> avgLatency, HashMap<Integer, Double> selectivity) {
		this.id = id;
		this.inputRecords = inputs;
		this.executedRecords = executed;
		this.outputsRecords = outputs;
		this.avgLatencyRecords = avgLatency;
		this.selectivityRecords = selectivity;
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
		orderedTimestamps.sort(new UtilFunctions.DecreasingIntOrder());
		return orderedTimestamps;
	}
	
	public static <T> T getRecord(HashMap<Integer, T> records, Integer rank){
		ArrayList<Integer> timestamps = ComponentWindowedStats.getRecordedTimestamps(records);
		Integer timestamp = timestamps.get(rank);
		return records.get(timestamp);
	}
	
	public static <T> T getLastRecord(HashMap<Integer, T> records){
		return ComponentWindowedStats.getRecord(records, 0);
	}
	
	public static <T> T getOldestRecord(HashMap<Integer, T> records){
		int oldestRank = ComponentWindowedStats.getRecordedTimestamps(records).size() - 1;
		return ComponentWindowedStats.getRecord(records, oldestRank);
	}


	//TODO define a method which takes records of a dimension and computes the list of variations 
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
	 * @return the outputsRecords
	 */
	public HashMap<Integer, Long> getOutputsRecords() {
		return outputsRecords;
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
}