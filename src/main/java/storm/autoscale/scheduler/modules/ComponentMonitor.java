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
	
	private static final String KEY_TIMESTAMP = "timestamp";
	private static final String KEY_COMPONENT = "component_id";
	private static final String KEY_TASK = "task_id";
	
	private final Connection connection;
	private final Statement statement;
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	
	/**
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * 
	 */
	public ComponentMonitor(String dbHost) throws ClassNotFoundException, SQLException {
		this.lastTimestamp = 0L;//TODO read and write to the database because it changes over the time
		this.step = 10L;
		this.currentTimestamp = this.lastTimestamp + this.step;
		
		this.components = new HashMap<>();
		
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
		this.statement = this.connection.createStatement();
	}
	
	public void update(){
		/*Get queue sizes for each component between the last and the current timestamp*/
		String queryQueues = "SELECT " + KEY_TIMESTAMP + ", " + KEY_COMPONENT + ","
				+ " SUM(" + INPUTS  + ") AS inputs, " + ", SUM(" + EXECUTED + ") AS executed, " + ", SUM(" + OUTPUTS  + ") AS outputs "
				+ "FROM " + TABLE_QUEUES + 
				" WHERE " + KEY_TIMESTAMP + " BETWEEN " + this.getLastTimestamp() + " AND " + this.getCurrentTimestamp() +
				" GROUP BY " + KEY_TIMESTAMP + ", " + KEY_COMPONENT;
		try {
			ResultSet resultQueues = this.statement.getResultSet();
			while(resultQueues.next()){
				String component = resultQueues.getString(KEY_COMPONENT);
				Double inputs = resultQueues.getDouble("inputs");
				Double executed = resultQueues.getDouble("executed");
				Double outputs = resultQueues.getDouble("outputs");
				Double selectivity = outputs / executed;
				//TODO put it into the component monitoring map
			}
		} catch (SQLException e) {
			logger.severe("Component queues' information can not be retrieved because " + e);
		}
		/*Get average latency for each component between the last and the current timestamp*/
		String queryLatency = "SELECT " + KEY_TIMESTAMP + ", " + KEY_COMPONENT + ","
				+ " AVG(" + LATENCY  + ") AS latency "
				+ "FROM " + TABLE_LATENCY + 
				" WHERE " + KEY_TIMESTAMP + " BETWEEN " + this.getLastTimestamp() + " AND " + this.getCurrentTimestamp() +
				" GROUP BY " + KEY_TIMESTAMP + ", " + KEY_COMPONENT;
		//TODO Get the result as done with queues
		/*Get average CPU load for each component between the last and the current timestamp*/
		String queryLoad = "SELECT " + KEY_TIMESTAMP + ", " + KEY_COMPONENT + ","
				+ " AVG(" + LOAD + ") AS load "
				+ "FROM " + TABLE_LOAD + 
				" WHERE " + KEY_TIMESTAMP + " BETWEEN " + this.getLastTimestamp() + " AND " + this.getCurrentTimestamp() +
				" GROUP BY " + KEY_TIMESTAMP + ", " + KEY_COMPONENT;
		//TODO Get the result as done with queues
	}
	
	public Long getLastTimestamp(){
		return this.lastTimestamp;
	}
	
	public Long getCurrentTimestamp(){
		return this.currentTimestamp;
	}
	
	public Double getInputQueueSize(String component){
		return 0.0;
		//TODO Give an easier access to the value
	}
	
	public Double getNbExecuted(String component){
		return 0.0;
		//TODO Give an easier access to the value
	}
	
	public Double getNbOutputs(String component){
		return 0.0;
		//TODO Give an easier access to the value
	}
	
	public Double getAvgLatency(String component){
		return 0.0;
		//TODO Give an easier access to the value
	}
	
	public Double getAvgCpuLoad(String component){
		return 0.0;
		//TODO Give an easier access to the value
	}
	
	public Double getEstimatedSelectivy(String component){
		return 0.0;
		//TODO Give an easier access to the value
	}
	
	public boolean isCongested(String component){
		return true;
		//TODO return if the inputs are greater than the executed tuples
	}
	
	public ArrayList<String> getCongested(){
		ArrayList<String> result = new ArrayList<>();
		//TODO get the keySet of the component map and for each component, add it to the result if it is congested
		return result;
	}

}
