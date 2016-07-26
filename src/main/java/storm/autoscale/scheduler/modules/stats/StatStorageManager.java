/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TFramedTransport;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import storm.autoscale.scheduler.modules.listener.NimbusListener;

/**
 * @author Roland
 *
 */
public class StatStorageManager implements Runnable{

	private static StatStorageManager manager = null;
	private NimbusListener listener;
	private Integer timestamp;
	private Integer rate;
	private HashMap<String, Boolean> topStatus;
	private final Connection connection;
	private final static String ALLTIME = ":all-time";
	private final static String TABLE_SPOUT = "all_time_spouts_stats";
	private final static String TABLE_BOLT = "all_time_bolts_stats";
	private final static String TABLE_TOPOLOGY = "topologies_status";
	private final static Integer LARGE_WINDOW_SIZE = 50;
	private Thread thread;
	private static Logger logger = Logger.getLogger("StatStorageManager");
	
	/**
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * 
	 */
	private StatStorageManager(String dbHost, String nimbusHost, Integer nimbusPort, Integer rate) throws SQLException, ClassNotFoundException {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
		this.listener = NimbusListener.getInstance(nimbusHost, nimbusPort);
		this.timestamp = 0;
		this.rate = rate;
		this.topStatus = new HashMap<>();
		this.thread = new Thread(this);
		try {
			thread.start();
			logger.fine("Statistic manager started successfully!");
		} catch (IllegalThreadStateException e) {
			logger.warning("Statistic storage manager has met an issue, restarting in few seconds...");
		}
	}
	
	private StatStorageManager(String dbHost) throws ClassNotFoundException, SQLException{
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
	}
	
	public static StatStorageManager getManager(String dbHost, String nimbusHost, Integer nimbusPort, Integer rate) throws ClassNotFoundException, SQLException{
		if(StatStorageManager.manager == null){
			StatStorageManager.manager = new StatStorageManager(dbHost, nimbusHost, nimbusPort, rate);
		}
		if(!manager.thread.isAlive()){
			manager.thread = new Thread(manager);
			manager.thread.start();
		}
		return StatStorageManager.manager;
	}
	
	public static StatStorageManager getManager(String dbHost) throws ClassNotFoundException, SQLException{
		if(StatStorageManager.manager == null){
			StatStorageManager.manager = new StatStorageManager(dbHost);
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
		return this.rate;
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
						
						if(!componentId.contains("acker")){ //avoiding to catch acker which are unsplittable
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
								Long outputs = 0L;
								for(String stream : emitted.keySet()){
									outputs += emitted.get(stream);
								}
								//We must explicitly test both cases
								Long outputsUpdate = outputs - this.getFormerValue(componentId, startTask, endTask, this.timestamp, "spout", "outputs");
								outputsUpdate = outputsUpdate - this.getFormerValue(componentId, startTask, endTask, this.timestamp, "bolt", "outputs");

								ExecutorSpecificStats specStats = stats.get_specific();

								/*Try to look if it is a spout*/

								if(specStats.is_set_spout()){
									
									SpoutStats spoutStats = specStats.get_spout();
									Map<String, Long> acked = spoutStats.get_acked().get(ALLTIME);
									Long throughput = 0L;
									for(String stream : acked.keySet()){
										throughput += acked.get(stream);
									}
									Long throughputUpdate = throughput - this.getFormerValue(componentId, startTask, endTask, this.timestamp, "spout", "throughput");
									
									Map<String, Long> failed = spoutStats.get_failed().get(ALLTIME);
									Long losses = 0L;
									for(String stream : failed.keySet()){
										losses += failed.get(stream);
									}
									Long lossesUpdate = losses - this.getFormerValue(componentId, startTask, endTask, this.timestamp, "spout", "losses");

									Map<String, Double> completeAvgTime = spoutStats.get_complete_ms_avg().get(ALLTIME);
									Double sum = 0.0;
									Double count = 0.0;
									for(String stream : completeAvgTime.keySet()){
										sum += completeAvgTime.get(stream);
										count++;
									}
									Double avgLatency = new BigDecimal(sum / count).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
									storeSpoutExecutorStats(this.getCurrentTimestamp(), host, port, topology.get_id(), componentId, startTask, endTask, outputsUpdate, throughputUpdate, lossesUpdate, avgLatency);
									logger.finest("Spout stats successfully persisted!");
								}


								if(specStats.is_set_bolt()){
									BoltStats boltStats = specStats.get_bolt();
									Map<GlobalStreamId, Long> executed = boltStats.get_executed().get(ALLTIME);
									Long nbExecuted = 0L;
									for(GlobalStreamId gs : executed.keySet()){
										nbExecuted += executed.get(gs);
									}
									Long executedUpdate = nbExecuted - this.getFormerValue(componentId, startTask, endTask, this.timestamp, "bolt", "executed");

									Map<GlobalStreamId, Double> executionAvgTime = boltStats.get_execute_ms_avg().get(ALLTIME);
									Double sum = 0.0;
									Double count = 0.0;
									for(GlobalStreamId gs : executionAvgTime.keySet()){
										sum += executionAvgTime.get(gs);
										count++;
									}
									Double avgLatency = new BigDecimal(sum / count).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

									Double selectivity = new BigDecimal(outputsUpdate / (executedUpdate * 1.0)).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
									storeBoltExecutorStats(this.getCurrentTimestamp(), host, port, topology.get_id(), componentId, startTask, endTask, executedUpdate, outputsUpdate, avgLatency, selectivity);
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

	public void storeSpoutExecutorStats(Integer timestamp, String host, Integer port, String topology, String component, Integer startTask, Integer endTask, Long outputs, Long throughput, Long losses, Double avgLatency){
		String query = "INSERT INTO " + TABLE_SPOUT + " VALUES('" + timestamp + "', ";
		query += "'" + host + "', " + "'" + port + "', " + "'" + topology + "', " + "'" + component + "', "
				+ "'" + startTask + "', " + "'" + endTask + "', "
					+ "'" + outputs + "', " + "'" + throughput + "', " + "'" + losses + "', " + "'" + avgLatency + "')";
		try {
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			statement.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store spout executor stats because of " + e);
		}
	}
	
	public void storeBoltExecutorStats(Integer timestamp, String host, Integer port, String topology, String component, Integer startTask, Integer endTask, Long executed, Long outputs, Double avgLatency, Double selectivity){
		String query = "INSERT INTO " + TABLE_BOLT + " VALUES('"  + timestamp + "', ";
		query += "'" + host + "', " + "'" + port + "', " + "'" + topology + "', " + "'" + component + "', "
				+ "'" + startTask + "', " + "'" + endTask + "', "
					+ "'" + executed + "', " + "'" + outputs + "', " + "'" + avgLatency + "', " + "'" + selectivity + "')";
		try {
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			statement.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store bolt executor stats because of " + e);
		}
	}
	
	public void storeTopologyState(Integer timestamp, String topology, String status){
		String query = "INSERT INTO " + TABLE_TOPOLOGY + " VALUES('" + timestamp + "', '" + topology + "', '" + status + "')";
		try{
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			statement.executeUpdate(query);
		}  catch (SQLException e) {
			logger.severe("Unable to store topology state because of " + e);
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
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultSpouts = statement.executeQuery(querySpouts);
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
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultBolts = statement.executeQuery(queryBolts);
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
			String query  = "SELECT timestamp, SUM(executed) AS nbExecuted FROM " + TABLE_BOLT
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp
					+ " GROUP BY timestamp, component";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
			String query  = "SELECT timestamp, SUM(outputs) AS nbOutputs FROM " + TABLE_SPOUT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
			String query  = "SELECT timestamp, SUM(outputs) AS nbOutputs FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
			String query  = "SELECT timestamp, AVG(execute_ms_avg) AS avgLatency FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
			String query  = "SELECT timestamp, AVG(selectivity) AS avgSelectivity FROM " + TABLE_BOLT 
					+ " WHERE component = '" + component 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
	
	public HashMap<Integer, Long> getTopologyThroughput(String topology, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(throughput) AS topThroughput FROM " + TABLE_SPOUT 
					+ " WHERE topology = '" + topology 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, topology;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
	
	public HashMap<Integer, Long> getTopologyLosses(String topology, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Long> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, SUM(losses) AS topLosses FROM " + TABLE_SPOUT 
					+ " WHERE topology = '" + topology 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp 
					+ " GROUP BY timestamp, topology;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
	
	public HashMap<Integer, Double> getTopologyAvgLatency(String topology, Integer timestamp, Integer windowSize){
		int oldestTimestamp =  timestamp - windowSize;
		HashMap<Integer, Double> records = new HashMap<>();
		try {
			String query  = "SELECT timestamp, MAX(complete_ms_avg) AS topLatency FROM " + TABLE_SPOUT 
					+ " WHERE topology = '" + topology 
					+ "' AND timestamp BETWEEN " + oldestTimestamp + " AND " + timestamp
					+ " GROUP BY timestamp, topology;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
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
					" WHERE timestamp BETWEEN " + oldestTimestamp + " AND " + previousTimestamp + 
					" AND start_task = " + startTask + " AND end_task = " + endTask;
			try {
				Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				ResultSet results = statement.executeQuery(query);
				if(results.last()){
					result = results.getLong(attribute);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover former value for executor associated to component " + component + "[tasks " + startTask + " to " + endTask + "] because " + e);
			}
		}
		if(componentType.equalsIgnoreCase("bolt")){
			String query = "SELECT " + attribute + " FROM " + TABLE_BOLT +
					" WHERE timestamp BETWEEN " + oldestTimestamp + " AND " + previousTimestamp + 
					" AND start_task = " + startTask + " AND end_task = " + endTask;
			try {
				Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				ResultSet results = statement.executeQuery(query);
				if(results.last()){
					result = results.getLong(attribute);
				}
			} catch (SQLException e) {
				logger.severe("Unable to recover former value for executor associated to component " + component + "[tasks " + startTask + " to " + endTask + "] because " + e);
			}
		}
		return result;
	}
	
	@Override
	public void run() {
		while(true){
			try {
				this.storeStatistics();
				Thread.sleep(this.getRate());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}	
}