/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

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
public class StatStorageManager extends Thread{

	private static StatStorageManager manager = null;
	private NimbusListener listener;
	private Integer timestamp;
	private Integer storedTimestamp;
	private Integer rate;
	private HashMap<String, Boolean> topStatus;
	private final Connection connection;
	private final static String ALLTIME = ":all-time";
	private final static String TABLE_SPOUT = "all_time_spouts_stats";
	private final static String TABLE_BOLT = "all_time_bolts_stats";
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
		this.storedTimestamp = Integer.MIN_VALUE;
		this.rate = rate;
		this.topStatus = new HashMap<>();
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
		//logger.info("Statistic collector started...");
		if(!StatStorageManager.manager.isAlive()){
			StatStorageManager.manager.start();
		}
		return StatStorageManager.manager;
	}
	
	public static StatStorageManager getManager(String dbHost) throws ClassNotFoundException, SQLException{
		if(StatStorageManager.manager == null){
			StatStorageManager.manager = new StatStorageManager(dbHost);
		}
		/*logger.info("Statistic collector started...");
		if(!StatStorageManager.manager.isAlive()){
			StatStorageManager.manager.start();
		}*/
		return StatStorageManager.manager;
	}
	
	public Nimbus.Client getClient(){
		return this.listener.getClient();
	}
	
	public Integer getCurrentTimestamp(){
		return this.timestamp;
	}
	
	public Integer getStoredTimestamp(){
		return this.storedTimestamp;
	}
	
	public Integer getRate(){
		return this.rate;
	}
	
	public Boolean isActive(String topId){
		return this.topStatus.get(topId);
	}
	
	public void storeStatistics(){
		TFramedTransport tTransport = this.listener.gettTransport();
		Nimbus.Client client = this.getClient();
		try {
			if(!tTransport.isOpen()){
				tTransport.open();
			}
			//logger.info("Listening to the Nimbus...");
			List<TopologySummary> topologies = client.getClusterInfo().get_topologies();
			for(TopologySummary topSummary : topologies){
				String topId = topSummary.get_id();
				boolean isActive = true;
				if(!topSummary.get_status().equalsIgnoreCase("ACTIVE")){
					isActive = false;
				}
				if(this.topStatus.containsKey(topId)){
					this.topStatus.replace(topId, isActive);
				}else{
					this.topStatus.put(topId, isActive);
				}
				if(isActive(topId)){
					this.timestamp = topSummary.get_uptime_secs();
					TopologyInfo topology = client.getTopologyInfo(topId);
					List<ExecutorSummary> executors = topology.get_executors();
					for(ExecutorSummary executor : executors){
						String componentId = executor.get_component_id();
						
						if(!componentId.contains("acker")){ //avoiding to catch acker which are unsplittable
							String host = executor.get_host();
							Integer port = executor.get_port();
							//logger.info("Retrieving data of component " + componentId + " on worker " + host + "@" + port + "...");

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

								ExecutorSpecificStats specStats = stats.get_specific();

								/*Try to look if it is a spout*/

								if(specStats.is_set_spout()){
									SpoutStats spoutStats = specStats.get_spout();
									Map<String, Long> acked = spoutStats.get_acked().get(ALLTIME);
									Long throughput = 0L;
									for(String stream : acked.keySet()){
										throughput += acked.get(stream);
									}

									Map<String, Long> failed = spoutStats.get_failed().get(ALLTIME);
									Long losses = 0L;
									for(String stream : failed.keySet()){
										losses += failed.get(stream);
									}

									Map<String, Double> completeAvgTime = spoutStats.get_complete_ms_avg().get(ALLTIME);
									Double sum = 0.0;
									Double count = 0.0;
									for(String stream : completeAvgTime.keySet()){
										sum += completeAvgTime.get(stream);
										count++;
									}
									Double avgLatency = sum / count;
									storeSpoutExecutorStats(this.getCurrentTimestamp(), host, port, topology.get_id(), componentId, startTask, endTask, outputs, throughput, losses, avgLatency);
									//logger.info("Spout stats successfully persisted!");
								}


								if(specStats.is_set_bolt()){
									BoltStats boltStats = specStats.get_bolt();
									Map<GlobalStreamId, Long> executed = boltStats.get_executed().get(ALLTIME);
									Long nbExecuted = 0L;
									for(GlobalStreamId gs : executed.keySet()){
										nbExecuted += executed.get(gs);
									}

									Map<GlobalStreamId, Double> executionAvgTime = boltStats.get_execute_ms_avg().get(ALLTIME);
									Double sum = 0.0;
									Double count = 0.0;
									for(GlobalStreamId gs : executionAvgTime.keySet()){
										sum += executionAvgTime.get(gs);
										count++;
									}
									Double avgLatency = sum / count;

									Double selectivity = outputs / (nbExecuted * 1.0);
									storeBoltExecutorStats(this.getCurrentTimestamp(), host, port, topology.get_id(), componentId, startTask, endTask, nbExecuted, outputs, avgLatency, selectivity);
									//logger.info("Bolt stats successfully persisted!");
								}
							}
							
						}else{
							logger.warning("Unable to identify the type of the operator");
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
	
	public ArrayList<String> getSpoutWorkers(String component, Integer timestamp){
		ArrayList<String> workers = new ArrayList<>();
		String querySpouts = "SELECT DISTINCT host, port FROM " + TABLE_SPOUT + " WHERE component = '" + component + "' AND timestamp = '" + timestamp + "';";
		try {
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultSpouts = statement.executeQuery(querySpouts);
			while(resultSpouts.next()){
				String worker = resultSpouts.getString("host") + "@" + resultSpouts.getString("port");
				workers.add(worker);
			}
			resultSpouts.close();
			if(workers.isEmpty()){
				logger.warning("Component " +  component + " seems to be unaffected or do not exist in running topologies");
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve assignments for component " + component + " because of " + e);
		}
		return workers;
	}
	
	public ArrayList<String> getBoltWorkers(String component, Integer timestamp){
		ArrayList<String> workers = new ArrayList<>();
		String queryBolts = "SELECT DISTINCT host, port FROM " + TABLE_BOLT + " WHERE component = '" + component + "' AND timestamp = '" + timestamp + "';";
		try {
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultBolts = statement.executeQuery(queryBolts);
			while(resultBolts.next()){
				String worker = resultBolts.getString("host") + "@" + resultBolts.getString("port");
				workers.add(worker);
			}			
			resultBolts.close();
			if(workers.isEmpty()){
				logger.warning("Component " +  component + " seems to be unaffected or do not exist in running topologies");
			}
		} catch (SQLException e) {
			logger.severe("Unable to retrieve assignments for component " + component + " because of " + e);
		}
		return workers;
	}
	
	public Long getExecuted(String component, Integer timestamp){
		Long result = 0L;
		try {
			String query  = "SELECT SUM(executed) AS nbExecuted FROM " + TABLE_BOLT + " WHERE component = '" + component + "' AND timestamp = " + timestamp + " GROUP BY component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
			while(results.next()){
				result = results.getLong("nbExecuted");
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the number of executed tuples of the component " + component + " because of " + e);
		}
		return result;
	}
	
	public Long getSpoutOutputs(String component, Integer timestamp){
		Long result = 0L;
		try {
			String querySpout  = "SELECT SUM(outputs) AS nbOutputs FROM " + TABLE_SPOUT + " WHERE component = '" + component + "' AND timestamp = " + timestamp + " GROUP BY component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultSpout = statement.executeQuery(querySpout);
			while(resultSpout.next()){
				result = resultSpout.getLong("nbOutputs");
			}	
			resultSpout.close();
			if(result == 0L){
				logger.warning("Component " +  component + " seems to emit no tuples or do not exist in running topologies");
			}
		} catch (SQLException e) {
			logger.severe("Unable to compute the number of executed tuples of the component " + component + " because of " + e);
		}
		return result;
	}
	
	public Long getBoltOutputs(String component, Integer timestamp){
		Long result = 0L;
		try {
			String queryBolt  = "SELECT SUM(outputs) AS nbOutputs FROM " + TABLE_BOLT + " WHERE component = '" + component + "' AND timestamp = " + timestamp + " GROUP BY component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultBolt = statement.executeQuery(queryBolt);
			while(resultBolt.next()){
				result = resultBolt.getLong("nbOutputs");
			}
			resultBolt.close();
			if(result == 0L){
				logger.warning("Component " +  component + " seems to emit no tuples or do not exist in running topologies");
			}
		} catch (SQLException e) {
			logger.severe("Unable to compute the number of executed tuples of the component " + component + " because of " + e);
		}
		return result;
	}
	
	public Double getAvgLatency(String component, Integer timestamp){
		Double result = 0.0;
		try {
			String query  = "SELECT AVG(execute_ms_avg) AS avgLatency FROM " + TABLE_BOLT + " WHERE component = '" + component + "' AND timestamp = " + timestamp + " GROUP BY component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
			while(results.next()){
				result = results.getDouble("avgLatency");				
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the average latency of the component " + component + " because of " + e);
		}
		return result;
	}
	
	public Double getSelectivity(String component, Integer timestamp){
		Double result = 0.0; 
		try {
			String query  = "SELECT AVG(selectivity) AS avgSelectivity FROM " + TABLE_BOLT + " WHERE component = '" + component + "' AND timestamp = " + timestamp + " GROUP BY component;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
			while(results.next()){
				result = results.getDouble("avgSelectivity");
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the average selectivity of the component " + component + " because of " + e);
		}
		return result;
	}
	
	public Long getTopologyThroughput(String topology, Integer timestamp){
		Long result = 0L;
		try {
			String query  = "SELECT SUM(throughput) AS topThroughput FROM " + TABLE_SPOUT + " WHERE topology = '" + topology + "' AND timestamp = " + timestamp + " GROUP BY topology;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
			while(results.next()){
				result = results.getLong("topThroughput");
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the global throughput of the topology " + topology + " because of " + e);
		}
		return result;
	}
	
	public Long getTopologyLosses(String topology, Integer timestamp){
		Long result = 0L;
		try {
			String query  = "SELECT SUM(losses) AS topLosses FROM " + TABLE_SPOUT + " WHERE topology = '" + topology + "' AND timestamp = " + timestamp + " GROUP BY topology;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
			while(results.next()){
				result = results.getLong("topLosses");
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the global losses of the topology " + topology + " because of " + e);
		}
		return result;
	}
	
	public Double getTopologyAvgLatency(String topology, Integer timestamp){
		Double result = 0.0;
		try {
			String query  = "SELECT MAX(complete_ms_avg) AS topLatency FROM " + TABLE_SPOUT + " WHERE topology = '" + topology + "' AND timestamp = " + timestamp + " GROUP BY topology;";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet results = statement.executeQuery(query);
			while(results.next()){
				result = results.getDouble("topLatency");
			}
			results.close();
		} catch (SQLException e) {
			logger.severe("Unable to compute the global latency of the topology " + topology + " because of " + e);
		}
		return result;
	}
	
	@Override
	public void run() {
		while(true){
			try {
				storeStatistics();
				Thread.sleep(this.getRate());
				if(this.getCurrentTimestamp() > 0){
					this.storedTimestamp = this.getCurrentTimestamp();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}	
}