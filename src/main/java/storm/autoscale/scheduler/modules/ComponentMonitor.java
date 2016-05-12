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
		this.statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		this.initTimestamp();
	}
	
	public void initTimestamp(){
		String queryLastTimestamp = "SELECT " + KEY_TIMESTAMP + " FROM " + TABLE_TIMESTAMP;
		String queryCurrentTimestamp = "SELECT " + KEY_TIMESTAMP + " FROM " + TABLE_QUEUES;
		try {
			ResultSet resultLastTimestamp = this.statement.executeQuery(queryLastTimestamp);
			if(resultLastTimestamp.last()){
				this.lastTimestamp = resultLastTimestamp.getLong(KEY_TIMESTAMP);
				ResultSet resultCurrentTimestamp = this.statement.executeQuery(queryCurrentTimestamp);
				if(resultCurrentTimestamp.last()){
					this.currentTimestamp = resultCurrentTimestamp.getLong(KEY_TIMESTAMP);
				}else{
					this.currentTimestamp = this.lastTimestamp;
				}
			}else{
				String queryGetFirstTimestamp = "SELECT " + KEY_TIMESTAMP + " FROM " + TABLE_QUEUES;
				ResultSet resultFirstTimestamp = this.statement.executeQuery(queryGetFirstTimestamp);
				if(resultFirstTimestamp.first()){
					this.lastTimestamp = resultFirstTimestamp.getLong(KEY_TIMESTAMP);
					if(resultFirstTimestamp.last()){
						this.currentTimestamp = resultFirstTimestamp.getLong(KEY_TIMESTAMP);
					}
				}else{
					this.lastTimestamp = 0L;
					this.currentTimestamp = 0L;
				}
				String queryInitTimestamp = "INSERT INTO " + TABLE_TIMESTAMP + " VALUES('" + this.getCurrentTimestamp() + "')";
				this.statement.executeUpdate(queryInitTimestamp);
			}
		} catch (SQLException e) {
			logger.severe("Scheduler timestamp can not be retrieved because " + e);
		}
	}
	
	public void updateTimestamp(){
		String queryUpdateTimestamp = "INSERT INTO " + TABLE_TIMESTAMP + " VALUES(" + this.getCurrentTimestamp() + ")";
		try {
			this.statement.executeUpdate(queryUpdateTimestamp);
		} catch (SQLException e) {
			logger.severe("Scheduler timestamp can not be updated because " + e);
		}
	}
	
	public void update(){
		/*Get queue sizes for each component between the last and the current timestamp*/
		String queryQueues = "SELECT " + KEY_COMPONENT + ","
				+ " SUM(" + INPUTS  + ") AS inputs, " + "SUM(" + EXECUTED + ") AS executed, " + "SUM(" + OUTPUTS  + ") AS outputs "
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
				+ " AVG(" + LATENCY  + ") AS avgLatency "
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
				Double latency = resultLatency.getDouble("avgLatency");
				monitorData.put(LATENCY, latency);
				this.components.replace(component, monitorData);
			}
		}catch (SQLException e){
			logger.severe("Component latencies' information can not be retrieved because " + e);
		}
		
		/*Get average CPU load for each component between the last and the current timestamp*/
		String queryLoad = "SELECT " + KEY_COMPONENT + ","
				+ " AVG(" + LOAD + ") AS avgLoad "
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
				Double load = resultLoad.getDouble("avgLoad");
				monitorData.put(LOAD, load);
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
	
	public ArrayList<String> getComponents(){
		ArrayList<String> result = new ArrayList<>();
		for(String component : this.components.keySet()){
			result.add(component);
		}
		return result;
	}
	
	public Double getInputQueueSize(String component){
		return this.components.get(component).get(INPUTS);
	}
	
	public void setInputQueueSize(String component, Double value){
		HashMap<String, Double> monitorData = this.components.get(component);
		monitorData.replace(INPUTS, value);
		this.components.replace(component, monitorData);
	}
	
	public Double getNbExecuted(String component){
		return this.components.get(component).get(EXECUTED);
	}
	
	public void setNbExecuted(String component, Double value){
		HashMap<String, Double> monitorData = this.components.get(component);
		monitorData.replace(EXECUTED, value);
		this.components.replace(component, monitorData);
	}
	
	public Double getNbOutputs(String component){
		return this.components.get(component).get(OUTPUTS);
	}
	
	public void setOutputQueueSize(String component, Double value){
		HashMap<String, Double> monitorData = this.components.get(component);
		monitorData.replace(OUTPUTS, value);
		this.components.replace(component, monitorData);
	}
	
	public Double getAvgLatency(String component){
		return this.components.get(component).get(LATENCY);
	}
	
	public void setAvgLatency(String component, Double value){
		HashMap<String, Double> monitorData = this.components.get(component);
		monitorData.replace(LATENCY, value);
		this.components.replace(component, monitorData);
	}
	
	public Double getAvgCpuLoad(String component){
		return this.components.get(component).get(LOAD);
	}
	
	public void setAvgCpuLoad(String component, Double value){
		HashMap<String, Double> monitorData = this.components.get(component);
		monitorData.replace(LOAD, value);
		this.components.replace(component, monitorData);
	}
	
	public Double getEstimatedSelectivy(String component){
		return this.components.get(component).get(SELECTIVITY);
	}
	
	public void setEstimatedSelectivity(String component, Double value){
		HashMap<String, Double> monitorData = this.components.get(component);
		monitorData.replace(SELECTIVITY, value);
		this.components.replace(component, monitorData);
	}
	
	public boolean isCongested(String component){
		Double inputs = this.getInputQueueSize(component);
		Double nbExecuted = this.getNbExecuted(component);
		if(inputs == null || nbExecuted == null){
			return false;
		}else{
			return inputs > nbExecuted;
		}
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
