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

import storm.autoscale.scheduler.UtilFunctions;

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
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger("ComponentWindowedStats");
	
	/**
	 * 
	 */
	public ComponentWindowedStats(String id, HashMap<Integer, Long> inputs, HashMap<Integer, Long> executed, HashMap<Integer, Long> outputs, HashMap<Integer, Double> avgLatency, HashMap<Integer, Double> selectivity) {
		this.id = id;
		this.inputRecords = inputs;
		this.executedRecords = executed;
		this.outputRecords = outputs;
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
}