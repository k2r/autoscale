/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author Roland
 *
 */
public class ComponentMonitor {

	private Long lastTimestamp;
	private Long currentTimestamp;
	private final Long step;
	
	private HashMap<String, HashMap<String, Double>> components;
	
	private static final String INPUTS = "inputs";
	private static final String EXECUTED = "nb_executed";
	private static final String OUTPUTS = "outputs";
	private static final String LATENCY = "average_latency";
	private static final String LOAD = "average_cpu_load";
	private static final String SELECTIVITY = "selectivity";
	
	private static final String TABLE_QUEUES = "operator_queues";
	private static final String TABLE_LATENCY = "operator_latencies";
	private static final String TABLE_LOAD = "operator_cpu_loads";
	private static final String TABLE_TIMESTAMP = "scheduler_timestamp";
	
	private static final String KEY_TIMESTAMP = "timestamp";
	private static final String KEY_COMPONENT = "component_id";
	
	private final Connection connection;
	private final Statement statement;
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	
	/**
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * 
	 */
	public ComponentMonitor(String dbHost) throws ClassNotFoundException, SQLException {
		this.components = new HashMap<>();
		
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
		this.statement = this.connection.createStatement();
		
		this.step = 10L;
		this.initTimestamp();
	}
	
	public void initTimestamp(){
		String queryTimestamp = "SELECT " + KEY_TIMESTAMP + " FROM " + TABLE_TIMESTAMP;
		try {
			ResultSet resultTimestamp = this.statement.executeQuery(queryTimestamp);
			if(resultTimestamp.last()){
				this.lastTimestamp = resultTimestamp.getLong(KEY_TIMESTAMP);
				this.currentTimestamp = this.lastTimestamp + this.step;
			}else{
				this.lastTimestamp = 0L;
				this.currentTimestamp = this.lastTimestamp + this.step;
				String queryInitTimestamp = "INSERT INTO " + TABLE_TIMESTAMP + " VALUES('" + this.getCurrentTimestamp() + "')";
				this.statement.executeQuery(queryInitTimestamp);
			}
		} catch (SQLException e) {
			logger.severe("Scheduler timestamp can not be retrieved because " + e);
		}
	}
	
	public void updateTimestamp(){
		String queryUpdateTimestamp = "INSERT INTO " + TABLE_TIMESTAMP + " VALUES('" + this.getCurrentTimestamp() + "')";
		try {
			this.statement.executeQuery(queryUpdateTimestamp);
		} catch (SQLException e) {
			logger.severe("Scheduler timestamp can not be updated because " + e);
		}
	}
	
	public void update(){
		/*Get queue sizes for each component between the last and the current timestamp*/
		String queryQueues = "SELECT " + KEY_COMPONENT + ","
				+ " SUM(" + INPUTS  + ") AS inputs, " + ", SUM(" + EXECUTED + ") AS executed, " + ", SUM(" + OUTPUTS  + ") AS outputs "
				+ "FROM " + TABLE_QUEUES + 
				" WHERE " + KEY_TIMESTAMP + " BETWEEN " + this.getLastTimestamp() + " AND " + this.getCurrentTimestamp() +
				" GROUP BY " + KEY_COMPONENT;
		try {
			ResultSet resultQueues = this.statement.executeQuery(queryQueues);
			while(resultQueues.next()){
				String component = resultQueues.getString(KEY_COMPONENT);
				Double inputs = resultQueues.getDouble("inputs");
				Double executed = resultQueues.getDouble("executed");
				Double outputs = resultQueues.getDouble("outputs");
				Double selectivity = outputs / executed;
				HashMap<String, Double> monitorData = new HashMap<>();
				monitorData.put(INPUTS, inputs);
				monitorData.put(EXECUTED, executed);
				monitorData.put(OUTPUTS, outputs);
				monitorData.put(SELECTIVITY, selectivity);
				this.components.put(component, monitorData);
			}
		} catch (SQLException e) {
			logger.severe("Component queues' information can not be retrieved because " + e);
		}
		
		/*Get average latency for each component between the last and the current timestamp*/
		String queryLatency = "SELECT " + KEY_COMPONENT + ","
				+ " AVG(" + LATENCY  + ") AS latency "
				+ "FROM " + TABLE_LATENCY + 
				" WHERE " + KEY_TIMESTAMP + " BETWEEN " + this.getLastTimestamp() + " AND " + this.getCurrentTimestamp() +
				" GROUP BY " + KEY_COMPONENT;
		try {
			ResultSet resultLatency = this.statement.executeQuery(queryLatency);
			while(resultLatency.next()){
				String component = resultLatency.getString(KEY_COMPONENT);
				if(!this.components.containsKey(component)){
					HashMap<String, Double> monitorData = new HashMap<>();
					this.components.put(component, monitorData);
				}
				HashMap<String, Double> monitorData = this.components.get(component);
				Double latency = resultLatency.getDouble(LATENCY);
				monitorData.put(LATENCY, latency);
				this.components.replace(component, monitorData);
			}
		}catch (SQLException e){
			logger.severe("Component latencies' information can not be retrieved because " + e);
		}
		
		/*Get average CPU load for each component between the last and the current timestamp*/
		String queryLoad = "SELECT " + KEY_COMPONENT + ","
				+ " AVG(" + LOAD + ") AS load "
				+ "FROM " + TABLE_LOAD + 
				" WHERE " + KEY_TIMESTAMP + " BETWEEN " + this.getLastTimestamp() + " AND " + this.getCurrentTimestamp() +
				" GROUP BY " + KEY_COMPONENT;
		try {
			ResultSet resultLoad = this.statement.executeQuery(queryLoad);
			while(resultLoad.next()){
				String component = resultLoad.getString(KEY_COMPONENT);
				if(!this.components.containsKey(component)){
					HashMap<String, Double> monitorData = new HashMap<>();
					this.components.put(component, monitorData);
				}
				HashMap<String, Double> monitorData = this.components.get(component);
				Double load = resultLoad.getDouble(LATENCY);
				monitorData.put(LATENCY, load);
				this.components.replace(component, monitorData);
			}
		}catch (SQLException e){
			logger.severe("Component loads' information can not be retrieved because " + e);
		}
		updateTimestamp();
	}
	
	public Long getLastTimestamp(){
		return this.lastTimestamp;
	}
	
	public Long getCurrentTimestamp(){
		return this.currentTimestamp;
	}
	
	public Double getInputQueueSize(String component){
		return this.components.get(component).get(INPUTS);
	}
	
	public Double getNbExecuted(String component){
		return this.components.get(component).get(EXECUTED);
	}
	
	public Double getNbOutputs(String component){
		return this.components.get(component).get(OUTPUTS);
	}
	
	public Double getAvgLatency(String component){
		return this.components.get(component).get(LATENCY);
	}
	
	public Double getAvgCpuLoad(String component){
		return this.components.get(component).get(LOAD);
	}
	
	public Double getEstimatedSelectivy(String component){
		return this.components.get(component).get(SELECTIVITY);
	}
	
	public boolean isCongested(String component){
		return this.getInputQueueSize(component) > this.getNbExecuted(component);
	}
	
	public ArrayList<String> getCongested(){
		ArrayList<String> result = new ArrayList<>();
		Set<String> components = this.components.keySet();
		for(String component : components){
			if(this.isCongested(component)){
				result.add(component);
			}
		}
		return result;
	}

}
