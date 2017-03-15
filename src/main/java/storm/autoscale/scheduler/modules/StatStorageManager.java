/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TFramedTransport;

import storm.autoscale.scheduler.connector.database.IJDBCConnector;
import storm.autoscale.scheduler.connector.database.MySQLConnector;
import storm.autoscale.scheduler.connector.nimbus.NimbusListener;

import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.Component;
/**
 * @author Roland
 *
 */
public class StatStorageManager{

	private static StatStorageManager manager = null;
	private NimbusListener listener;
	private IJDBCConnector connector;
	private Integer timestamp;
	private Integer rate;
	private HashMap<String, Boolean> topStatus;
	private final static String ALLTIME = ":all-time";
	
	public final static String TABLE_SPOUT = "all_time_spouts_stats";
	public final static String TABLE_BOLT = "all_time_bolts_stats";
	public final static String TABLE_TOPOLOGY = "topologies_status";
	public final static String TABLE_ACTIVITY = "operators_activity";
	public final static String TABLE_CONSTRAINT = "operators_constraints";
	
	private final static String COL_TOTAL_EXEC = "total_executed";
	private final static String COL_TOTAL_OUTPUT = "total_outputs";
	private final static String COL_TOTAL_THROUGHPUT = "total_throughput";
	private final static String COL_TOTAL_LOSS = "total_losses";
	private final static String COL_UPDT_EXEC = "update_executed";
	private final static String COL_UPDT_OUTPUT = "update_outputs";
	private final static String COL_UPDT_THROUGHPUT = "update_throughput";
	private final static String COL_UPDT_LOSS = "update_losses";
	private final static String COL_AVG_LATENCY = "execute_ms_avg";
	private final static String COL_SELECTIVITY = "selectivity";
	private final static String COL_CPU_USAGE = "cpu_usage";
	private final static String COL_CPU_CONS = "cpu";
	private final static String COL_MEM_CONS = "memory";
	
	private final static Integer LARGE_WINDOW_SIZE = 120;
	
	
	//private Thread thread;
	private static Logger logger = Logger.getLogger("StatStorageManager");
	
	/**
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * 
	 */
	private StatStorageManager(String dbHost, String dbName, String dbUser, String password, String nimbusHost, Integer nimbusPort, Integer rate) throws SQLException, ClassNotFoundException {
		this.connector = new MySQLConnector(dbHost, dbName, dbUser, password);
		this.listener = NimbusListener.getInstance(nimbusHost, nimbusPort);
		this.timestamp = 0;
		this.rate = rate;
		this.topStatus = new HashMap<>();
	}
	
	private StatStorageManager(String dbHost, String dbName, String dbUser, String password) throws ClassNotFoundException, SQLException{
		this.connector = new MySQLConnector(dbHost, dbName, dbUser, password);
	}
	
	public static StatStorageManager getManager(String dbHost, String dbName, String dbUser, String password, String nimbusHost, Integer nimbusPort, Integer rate) throws ClassNotFoundException, SQLException{
		if(StatStorageManager.manager == null){
			StatStorageManager.manager = new StatStorageManager(dbHost, dbName, dbUser, password , nimbusHost, nimbusPort, rate);
		}
		return StatStorageManager.manager;
	}
	
	public static StatStorageManager getManager(String dbHost, String dbName, String dbUser, String password) throws ClassNotFoundException, SQLException{
		if(StatStorageManager.manager == null){
			StatStorageManager.manager = new StatStorageManager(dbHost, dbName, dbUser, password);
		}
		return StatStorageManager.manager;
	}
	
	public Nimbus.Client getClient(){
		return this.listener.getClient();
	}
	
	public Integer getCurrentTimestamp(){
		return this.timestamp;
	}
	
	public Integer getRate(){
		//turn it into milliseconds
		return this.rate * 1000;
	}
	
	public Boolean isActive(String topId){
		boolean active = false;
		try{
			active = this.topStatus.get(topId);
		}catch(NullPointerException e){
			logger.fine("Topology " + topId + " has never been activated yet!");
		}
		return active;
	}
	
	public void storeStatistics(){
		TFramedTransport tTransport = this.listener.gettTransport();
		Nimbus.Client client = this.getClient();
		try {
			if(!tTransport.isOpen()){
				tTransport.open();
			}
			logger.finest("Listening to the Nimbus...");
			List<TopologySummary> topologies = client.getClusterInfo().get_topologies();
			
			for(TopologySummary topSummary : topologies){
				String topId = topSummary.get_id();
				this.timestamp = topSummary.get_uptime_secs();
				boolean isActive = true;
				String topStatus = topSummary.get_status();
				if(!topStatus.equalsIgnoreCase("ACTIVE")){
					isActive = false;
				}
				if(this.topStatus.containsKey(topId)){
					this.topStatus.remove(topId);
				}
				this.topStatus.put(topId, isActive);
				storeTopologyState(this.timestamp, topId, topStatus);
				if(isActive(topId)){	
					TopologyInfo topology = client.getTopologyInfo(topId);
					List<ExecutorSummary> executors = topology.get_executors();
					for(ExecutorSummary executor : executors){
						String componentId = executor.get_component_id();

						if(!componentId.contains("acker") && !componentId.contains("eventlog")){ //avoiding to catch acker and eventlogger which are unsplittable
							String host = executor.get_host();
							Integer port = executor.get_port();
							logger.finest("Retrieving data of component " + componentId + " on worker " + host + "@" + port + "...");

							ExecutorInfo info = executor.get_executor_info();
							Integer startTask = info.get_task_start();
							Integer endTask = info.get_task_end();
							ExecutorStats stats = executor.get_stats();
							
							if(stats != null){
								/*Get outputs independently of the output stream and from the start of the topology*/
								Map<String, Long> emitted = stats.get_emitted().get(ALLTIME);
								Long totalOutputs = 0L;
								try{
									for(String stream : emitted.keySet()){
										if(!stream.contains("ack") && !stream.contains("eventlog")){
											totalOutputs += emitted.get(stream);
										}
									}
								}catch(NullPointerException e){
									logger.warning("Emission logs does not exist yet for component " + componentId);
								}
								ExecutorSpecificStats specStats = stats.get_specific();

								/*Try to look if it is a spout*/

								if(specStats.is_set_spout()){

									Long formerOutputs = this.getFormerValue(componentId, startTask, endTask, this.timestamp, "spout", COL_TOTAL_OUTPUT);
									Long updateOutputs = totalOutputs;
									if(formerOutputs <= updateOutputs){
										updateOutputs = updateOutputs - formerOutputs;
									}

									SpoutStats spoutStats = specStats.get_spout();
									Map<String, Long> acked = spoutStats.get_acked().get(ALLTIME);
									Long totalThroughput = 0L;
									try{
										for(String stream : acked.keySet()){
											totalThroughput += acked.get(stream);
										}
									}catch(NullPointerException e){
										logger.warning("Ack logs does not exist yet for component " + componentId);
									}

									Long formerThroughput = this.getFormerValue(componentId, startTask, endTask, this.timestamp, "spout", COL_TOTAL_THROUGHPUT);
									Long updateThroughput = totalThroughput;
									if(formerThroughput <= updateThroughput){
										updateThroughput = updateThroughput - formerThroughput;
									}

									Map<String, Long> failed = spoutStats.get_failed().get(ALLTIME);
									Long totalLosses = 0L;
									try{
										for(String stream : failed.keySet()){
											totalLosses += failed.get(stream);
										}
									}catch(NullPointerException e){
										logger.warning("Failure logs does not exist yet for component " + componentId);
									}

									Long formerLosses = this.getFormerValue(componentId, startTask, endTask, this.timestamp, "spout", COL_TOTAL_LOSS);
									Long updateLosses = totalLosses;
									if(formerLosses <= updateLosses){
										updateLosses = updateLosses - formerLosses;
									}

									Map<String, Double> completeAvgTime = spoutStats.get_complete_ms_avg().get(ALLTIME);
									Double sum = 0.0;
									Double count = 0.0;
									try{
										for(String stream : completeAvgTime.keySet()){
											sum += completeAvgTime.get(stream);
											count++;
										}
									}catch(NullPointerException e){
										logger.warning("Latency logs does not exist yet for component " + componentId);
									}

									Double avgLatency  = sum / count;
									if(avgLatency.isInfinite() || avgLatency.isNaN()){
										avgLatency = 0.0;
									}else{
										avgLatency = new BigDecimal(sum / count).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
									}


									storeSpoutExecutorStats(this.getCurrentTimestamp(), host, port, topology.get_id(), componentId, startTask, endTask, totalOutputs, updateOutputs, totalThroughput, updateThroughput, totalLosses, updateLosses, avgLatency);
									logger.finest("Spout stats successfully persisted!");

								}


								if(specStats.is_set_bolt()){

									Long formerOutputs = this.getFormerValue(componentId, startTask, endTask, this.timestamp, "bolt", COL_TOTAL_OUTPUT);
									Long updateOutputs = totalOutputs;
									if(formerOutputs <= updateOutputs){
										updateOutputs = updateOutputs - formerOutputs;
									}

									BoltStats boltStats = specStats.get_bolt();
									Map<GlobalStreamId, Long> executed = boltStats.get_executed().get(ALLTIME);
									Long totalExecuted = 0L;
									try{
										for(GlobalStreamId gs : executed.keySet()){
											if(!gs.get_streamId().contains("ack")){
												totalExecuted += executed.get(gs);
											}
										}
									}catch(NullPointerException e){
										logger.warning("Execution logs does not exist yet for component " + componentId);
									}

									Long formerExecuted = this.getFormerValue(componentId, startTask, endTask, this.timestamp, "bolt", COL_TOTAL_EXEC);
									Long updateExecuted = totalExecuted;
									if(formerExecuted <= updateExecuted){
										updateExecuted = updateExecuted - formerExecuted;
									}

									Map<GlobalStreamId, Double> executionAvgTime = boltStats.get_execute_ms_avg().get(ALLTIME);
									Double sum = 0.0;
									Double count = 0.0;

									try{
										for(GlobalStreamId gs : executionAvgTime.keySet()){
											if(!gs.get_streamId().contains("ack")){
												sum += executionAvgTime.get(gs);
												count++;
											}
										}
									}catch(NullPointerException e){
										logger.warning("Latency logs does not exist yet for component " + componentId);
									}

									Double avgLatency = sum / count;
									if(avgLatency.isInfinite() || avgLatency.isNaN()){
										avgLatency = 0.0;
									}else{
										avgLatency = new BigDecimal(sum / count).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
									}

									Double selectivity = totalOutputs / (totalExecuted * 1.0);
									if(selectivity.isInfinite() || selectivity.isNaN()){
										selectivity = 0.0;
									}else{
										selectivity = new BigDecimal(selectivity).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
									}
									
									Double cpuUsage = ((updateExecuted * avgLatency) / (rate * 1000)) * 100;//convert rate into ms
									
									storeBoltExecutorStats(this.getCurrentTimestamp(), host, port, topology.get_id(), componentId, startTask, endTask, totalExecuted, updateExecuted, totalOutputs, updateOutputs, avgLatency, selectivity, cpuUsage);
									logger.finest("Bolt stats successfully persisted!");
								}

							}

						}else{
							logger.fine("Unable to identify the type of the operator");
						}
					}
				}
				tTransport.close();
			}
		} catch (TException e) {
			e.printStackTrace();
		}
	}			

	public void storeSpoutExecutorStats(Integer timestamp, String host, Integer port, String topology, String component, Integer startTask, Integer endTask, Long totalOutputs, Long updateOutputs, Long totalThroughput, Long updateThroughput, Long totalLosses, Long updateLosses, Double avgLatency){
		String query = "INSERT INTO " + TABLE_SPOUT + " VALUES('" + timestamp + "', ";
		query += "'" + host + "', " + "'" + port + "', " + "'" + topology + "', " + "'" + component + "', "
				+ "'" + startTask + "', " + "'" + endTask + "', "
					+ "'" + totalOutputs + "', " + "'" + updateOutputs + "', " + "'" + totalThroughput + "', " + "'" + updateThroughput + "', " 
						+ "'" + totalLosses + "', " + "'" + updateLosses + "', " + "'" + avgLatency + "')";
		try {
			this.connector.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store spout executor stats because of " + e);
		}
	}
	
	public void storeBoltExecutorStats(Integer timestamp, String host, Integer port, String topology, String component, Integer startTask, Integer endTask, Long totalExecuted, Long updateExecuted, Long totalOutputs, Long updateOutputs,  Double avgLatency, Double selectivity, Double cpuUsage){
		String query = "INSERT INTO " + TABLE_BOLT + " VALUES('"  + timestamp + "', ";
		query += "'" + host + "', " + "'" + port + "', " + "'" + topology + "', " + "'" + component + "', "
				+ "'" + startTask + "', " + "'" + endTask + "', "
					+ "'" + totalExecuted + "', " + "'" + updateExecuted + "', " + "'" + totalOutputs + "', " + "'" + updateOutputs + "', "
						+ "'" + avgLatency + "', " + "'" + selectivity + "', " + "'" + cpuUsage + "')";
		try {
			this.connector.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store bolt executor stats because of " + e);
		}
	}
	
	public void storeTopologyState(Integer timestamp, String topology, String status){
		String query = "INSERT INTO " + TABLE_TOPOLOGY + " VALUES('" + timestamp + "', '" + topology + "', '" + status + "')";
		try{
			this.connector.executeUpdate(query);
		}  catch (SQLException e) {
			logger.severe("Unable to store topology state because of " + e);
		}
	}
	
	public void storeTopologyConstraints(Integer timestamp, TopologyDetails topology){
		Map<String, Component> components = topology.getComponents();
		for(String compName : components.keySet()){
			Component component = components.get(compName);
			List<ExecutorDetails> executors = component.execs;
			Double cpuReq = 0.0;
			Double memReq = 0.0;
			for(ExecutorDetails exec : executors){
				cpuReq = topology.getTotalCpuReqTask(exec);
				memReq = topology.getTotalMemReqTask(exec);
				break;
			}
			String type = "";
			if(this.isInitialConstraint(timestamp, topology.getName(), component.id)){
				type = "initial";
			}else{
				type = "update";
			}
			
			String query = "INSERT INTO " + TABLE_CONSTRAINT + " VALUES ('" + timestamp + "', '"
					+ topology.getName() + "', '" + component.id + "', '" + type + "', '" + cpuReq + "', '" + memReq + "')";
			try{
				this.connector.executeUpdate(query);
			} catch (SQLException e){
				logger.fine("Unable to store topology constraints because of " + e);
			}
		}
	}
	
	public void storeActivityInfo(Integer timestamp, String topology, String component, Double cr, Integer remaining, Double capacity, Double estimatedLoad){
		String query = "INSERT INTO " + TABLE_ACTIVITY + " VALUES ('" + timestamp + "', '"
				+ topology + "', '" + component + "', '" + cr + "', '" + remaining + "', '" + capacity + "', '" + estimatedLoad + "')";
		try{
			this.connector.executeUpdate(query);
		} catch (SQLException e){
			logger.fine("Unable to store activity info because of " + e);
		}
	}
	
	public HashMap<Integer, ArrayList<String>> getSpoutWorkers(String component, Integer timestamp, Integer windowSize){
		HashMap<Integer, ArrayList<String>> records = new HashMap<>();
		int oldestTimestamp = timestamp - windowSize;
		String querySpouts = "SELECT DISTINCT timestamp, host, port FROM " + TABLE_SPOUT 
				+ " WHERE component = '" + component + "' "
				+ "AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp + " "
				+ "ORDER BY timestamp;";
		try {
			ResultSet resultSpouts = this.connector.executeQuery(querySpouts);
			int currentTimestamp = 0;
			ArrayList<String> workers = new ArrayList<>();
			while(resultSpouts.next()){
				int readTimestamp = resultSpouts.getInt("timestamp");
				if(readTimestamp > currentTimestamp){
					if(!workers.isEmpty()){
						records.put(currentTimestamp, workers);
					}
					currentTimestamp = readTimestamp;
					workers = new ArrayList<>();
				}
				String worker = resultSpouts.getString("host") + "@" + resultSpouts.getString("port");
				workers.add(worker);
			}
			records.put(currentTimestamp, workers);
			resultSpouts.close();
			if(records.isEmpty()){
				logger.warning("Component " +  component + " seems to be unaffected or do not exist in running topologies");
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve assignments for component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, ArrayList<String>> getBoltWorkers(String component, Integer timestamp, Integer windowSize){
		HashMap<Integer, ArrayList<String>> records = new HashMap<>();
		int oldestTimestamp =  timestamp - windowSize;
		String queryBolts = "SELECT DISTINCT timestamp, host, port FROM " + TABLE_BOLT 
				+ " WHERE component = '" + component + "' "
				+ "AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp + " "
				+ "ORDER BY timestamp;";
		try {
			ResultSet resultBolts = this.connector.executeQuery(queryBolts);
			int currentTimestamp = 0;
			ArrayList<String> workers = new ArrayList<>();
			while(resultBolts.next()){
				int readTimestamp = resultBolts.getInt("timestamp");
				if(readTimestamp > currentTimestamp){
					if(!workers.isEmpty()){
						records.put(currentTimestamp, workers);
					}
					currentTimestamp = readTimestamp;
					workers = new ArrayList<>();
				}
				String worker = resultBolts.getString("host") + "@" + resultBolts.getString("port");
				workers.add(worker);
			}
			records.put(currentTimestamp, workers);
			resultBolts.close();
			if(records.isEmpty()){
				logger.warning("Component " +  component + " seems to be unaffected or do not exist in running topologies");
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve assignments for component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Long> getExecuted(String component, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(" + COL_UPDT_EXEC + ") AS nbExecuted FROM " + TABLE_BOLT
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp
					+ " GROUP BY timestamp, component";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Long nbExecuted = results.getLong("nbExecuted");
				records.put(readTimestamp, nbExecuted);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the number of executed tuples of the component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Long> getSpoutOutputs(String component, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(" + COL_UPDT_OUTPUT + ") AS nbOutputs FROM " + TABLE_SPOUT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Long nbOutput = results.getLong("nbOutputs"); 
				records.put(readTimestamp, nbOutput);
			}	
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the number of executed tuples of the component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Long> getBoltOutputs(String component, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(" + COL_UPDT_OUTPUT + ") AS nbOutputs FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Long nbOutput = results.getLong("nbOutputs");
				records.put(readTimestamp, nbOutput);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the number of executed tuples of the component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Double> getAvgLatency(String component, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Double> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, AVG(" + COL_AVG_LATENCY + ") AS avgLatency FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Double avgLatency = results.getDouble("avgLatency");
				records.put(readTimestamp, avgLatency);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the average latency of the component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Double> getSelectivity(String component, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Double> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, AVG(" + COL_SELECTIVITY + ") AS avgSelectivity FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Double avgSelectivity = new BigDecimal(results.getDouble("avgSelectivity")).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
				records.put(readTimestamp, avgSelectivity);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the average selectivity of the component " + component + " because of " + e);
		}
		return records;
	}

	public HashMap<Integer, ArrayList<Double>> getCpuUsage(String component, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, ArrayList<Double>> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, " + COL_CPU_USAGE + " FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp;
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				ArrayList<Double> usages = new ArrayList<>();
				if(records.containsKey(readTimestamp)){
					usages = records.get(readTimestamp);
				}
				Double cpuUsage = new BigDecimal(results.getDouble(COL_CPU_USAGE)).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
				usages.add(cpuUsage);
				records.put(readTimestamp, usages);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to retrieve the average cpu usage of the component " + component + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Long> getTopologyLosses(String topology, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(" + COL_UPDT_LOSS + ") AS topLosses FROM " + TABLE_SPOUT 
					+ " WHERE topology = '" + topology 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, topology;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Long losses = results.getLong("topLosses");
				records.put(readTimestamp, losses);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the global losses of the topology " + topology + " because of " + e);
		}
		return records;
	}
	
	public HashMap<Integer, Long> getTopologyThroughput(String topology, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(" +  COL_UPDT_THROUGHPUT + ") AS topThroughput FROM " + TABLE_SPOUT 
					+ " WHERE topology = '" + topology 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, topology;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Long throughput = results.getLong("topThroughput");
				records.put(readTimestamp, throughput);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the global throughput of the topology " + topology + " because of " + e);
		}
		return records;
	}

	public HashMap<Integer, Double> getTopologyAvgLatency(String topology, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Double> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, MAX(complete_ms_avg) AS topLatency FROM " + TABLE_SPOUT 
					+ " WHERE topology = '" + topology 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp
					+ " GROUP BY timestamp, topology;";
			ResultSet results = this.connector.executeQuery(query);
			while(results.next()){
				Integer readTimestamp = results.getInt("timestamp");
				Double topLatency = results.getDouble("topLatency");
				records.put(readTimestamp, topLatency);
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the global latency of the topology " + topology + " because of " + e);
		}
		return records;
	}
	
	public Long getFormerValue(String component, Integer startTask, Integer endTask, Integer timestamp, String componentType, String attribute){
		Long result = 0L;
		Integer oldestTimestamp = timestamp - LARGE_WINDOW_SIZE;
		Integer previousTimestamp = timestamp - 1;
		if(componentType.equalsIgnoreCase("spout")){
			String query = "SELECT " + attribute + " FROM " + TABLE_SPOUT +
					" WHERE component = '" + component + "' " +
					" AND timestamp BETWEEN " + oldestTimestamp + " AND " + previousTimestamp + 
					" AND start_task = " + startTask + " AND end_task = " + endTask;
			try {
				ResultSet results = this.connector.executeQuery(query);
				if(results.last()){
					result = results.getLong(attribute);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover former value for executor associated to component " + component + "[tasks " + startTask + " to " + endTask + "] because " + e);
			}
		}
		if(componentType.equalsIgnoreCase("bolt")){
			String query = "SELECT " + attribute + " FROM " + TABLE_BOLT +
					" WHERE component = '" + component + "' " + 
					" AND timestamp BETWEEN " + oldestTimestamp + " AND " + previousTimestamp + 
					" AND start_task = " + startTask + " AND end_task = " + endTask;
			try {
				ResultSet results = this.connector.executeQuery(query);
				if(results.last()){
					result = results.getLong(attribute);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover former value for executor associated to component " + component + "[tasks " + startTask + " to " + endTask + "] because " + e);
			}
		}
		return result;
	}
	
	public Long getCurrentTotalOutput(Integer timestamp, String component, String table){
		Long result = 0L;
		String query = "SELECT timestamp, SUM(" + COL_TOTAL_OUTPUT + ") AS outputs FROM " + table +
				" WHERE component = '" + component + "' " + 
				" AND timestamp = " + timestamp +
				" GROUP BY timestamp, component";
		try {
			ResultSet results = this.connector.executeQuery(query);
			if(results.first()){
				result = results.getLong("outputs");
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve former value of total outcoming tuples because of " + e);
		}
		return result;
	}
	
	public Long getCurrentTotalExecuted(Integer timestamp, String component, String table){
		Long result = 0L;
		String query = "SELECT timestamp, SUM(" + COL_TOTAL_EXEC + ") AS executed FROM " + table +
				" WHERE component = '" + component + "' " + 
				" AND timestamp = " + timestamp +
				" GROUP BY timestamp, component";
		try {
			ResultSet results = this.connector.executeQuery(query);
			if(results.first()){
				result = results.getLong("executed");
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve former value of total executed tuples because of " + e);
		}
		return result;
	}
	
	public boolean existConstraint(String topology){
		boolean result = false;
		String query = "SELECT * FROM " + TABLE_CONSTRAINT + " WHERE topology = '" + topology + "';";
		try{
			ResultSet results = this.connector.executeQuery(query);
			if(results.next()){
				result = true;
			}
		}catch (SQLException e){
			logger.fine("Unable to check existence of constraints because of " + e);
		}
		return result;
	}
	
	public boolean isInitialConstraint(Integer timestamp, String topology, String component){
		boolean result = true;
		String query = "SELECT * FROM " + TABLE_CONSTRAINT + " WHERE timestamp < " + timestamp + 
				" AND topology = '" + topology + "' AND component = '" + component + "';";
		try{
			ResultSet results = this.connector.executeQuery(query);
			if(results.next()){
				result = false;
			}
		} catch (SQLException e){
			logger.fine("Unable to check initial constraints because of " + e);
		}
		return result;
	}
	
	public Double getInitialCpuConstraint(String topology, String component){
		Double result = 0.0;
		String query = "SELECT " + COL_CPU_CONS + " FROM " + TABLE_CONSTRAINT +
				" WHERE topology = '" + topology + "'" +
				" AND component = '" + component + "'" + 
				" AND type = 'initial';";
		try {
			ResultSet results = this.connector.executeQuery(query);
			if(results.first()){
				result = results.getDouble(COL_CPU_CONS);
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve the initial cpu constraint because of " + e);
		}
		return result;
	}
	
	public Double getInitialMemConstraint(String topology, String component){
		Double result = 0.0;
		String query = "SELECT " + COL_MEM_CONS + " FROM " + TABLE_CONSTRAINT +
				" WHERE topology = '" + topology + "'" +
				" AND component = '" + component + "'" + 
				" AND type = 'initial';";
		try {
			ResultSet results = this.connector.executeQuery(query);
			if(results.first()){
				result = results.getDouble(COL_MEM_CONS);
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve the initial memory constraint because of " + e);
		}
		return result;
	}
	
	public Double getCurrentCpuConstraint(String topology, String component){
		Double result = 0.0;
		String query = "SELECT oc1." + COL_CPU_CONS + " FROM " + TABLE_CONSTRAINT + " oc1 " +
				" WHERE oc1.topology = '" + topology + "'" +
				" AND oc1.component = '" + component + "'" + 
				" AND oc1.timestamp >= ALL (SELECT oc2.timestamp FROM " + TABLE_CONSTRAINT + " oc2);";
		try {
			ResultSet results = this.connector.executeQuery(query);
			if(results.first()){
				result = results.getDouble(COL_CPU_CONS);
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve the current cpu constraint because of " + e);
		}
		return result;
	}
	
	public Double getCurrentMemConstraint(String topology, String component){
		Double result = 0.0;
		String query = "SELECT oc1." + COL_MEM_CONS + " FROM " + TABLE_CONSTRAINT + " oc1 " +
				" WHERE oc1.topology = '" + topology + "'" +
				" AND oc1.component = '" + component + "'" + 
				" AND oc1.timestamp >= ALL (SELECT oc2.timestamp FROM " + TABLE_CONSTRAINT + " oc2);";
		try {
			ResultSet results = this.connector.executeQuery(query);
			if(results.first()){
				result = results.getDouble(COL_MEM_CONS);
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve the current memory constraint because of " + e);
		}
		return result;
	}
}