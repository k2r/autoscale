/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.allocation.IAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;

/**
 * @author Roland
 *
 */
public class ScaleInAction implements IAction {

	private ComponentMonitor compMonitor;
	private TopologyExplorer explorer;
	private AssignmentMonitor assignMonitor;
	private IAllocationStrategy allocStrategy;
	private String nimbusHost;
	private Integer nimbusPort;
	private String password;
	private Connection connection;
	private Set<String> validateActions;
	private Logger logger = Logger.getLogger("ScaleInAction");
	
	/**
	 * 
	 */
	public ScaleInAction(ComponentMonitor compMonitor, TopologyExplorer explorer,
			AssignmentMonitor assignMonitor, IAllocationStrategy allocStrategy, String nimbusHost,
			Integer nimbusPort, String password) {
		this.compMonitor = compMonitor;
		this.explorer = explorer;
		this.assignMonitor = assignMonitor;
		this.allocStrategy = allocStrategy;
		this.nimbusHost = nimbusHost;
		this.nimbusPort = nimbusPort;
		this.password = password;
		ArrayList<String> actions = this.compMonitor.getScaleInRequirements();
		this.validateActions = new HashSet<>();
		for(String action : actions){
			this.validateActions.add(action);
		}
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://localhost/benchmarks";
		try {
			Class.forName(jdbcDriver);
			this.connection = DriverManager.getConnection(dbUrl, "root", this.password);
		} catch (SQLException | ClassNotFoundException e) {
			logger.severe("Unable to scale in components because " + e);
		}
 		Thread thread = new Thread(this);
		thread.start();
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		//HashMap<String, WorkerSlot> bestWorkers = this.getBestLocation();
		TSocket tsocket = new TSocket(this.nimbusHost, this.nimbusPort);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		try {
			if(!tTransport.isOpen()){
				tTransport.open();
			}
			for(String component : this.validateActions){
				Double activityValue = this.compMonitor.getActivityValue(component);
	
				int maxParallelism = this.assignMonitor.getAllSortedTasks(component).size();

				int currentParallelism = this.assignMonitor.getParallelism(component);
				int estimatedParallelism  = Math.max(1, (int) Math.round(activityValue * currentParallelism));
		
				int newParallelism = Math.min(maxParallelism, estimatedParallelism);
				
				if(newParallelism < currentParallelism && currentParallelism > 1 && newParallelism > 0 && !isGracePeriod(component)){
					
					RebalanceOptions options = new RebalanceOptions();
					options.put_to_num_executors(component, newParallelism);
					options.set_num_workers(this.assignMonitor.getNbWorkers());
					options.set_wait_secs(0);

					logger.fine("Changing parallelism degree of component " + component + " from " + currentParallelism + " to " + newParallelism + "...");

					client.rebalance(explorer.getTopologyName(), options);
					logger.fine("Parallelism of component " + component + " decreased successfully!");
					storeAction(component, currentParallelism, newParallelism);
				}else{
					logger.fine("This scale-in will not decrease the distribution of the operator");
				}
				Thread.sleep(1000);
			}
		}catch (TException | InterruptedException e) {
			logger.fine("Unable to scale topology " + explorer.getTopologyName() + " because of " + e);
			//e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#getBestLocation()
	 */
	@Override
	public HashMap<String, WorkerSlot> getBestLocation() {
		HashMap<String, WorkerSlot> result = new HashMap<>();
		for(String component : this.validateActions){
			WorkerSlot bestWorker = null;
			Double bestScore = Double.NEGATIVE_INFINITY;
			HashMap<WorkerSlot, Double> scores = this.allocStrategy.getScores(component);
			for(WorkerSlot ws : scores.keySet()){
				Double score = scores.get(ws);
				if(score > bestScore){
					bestWorker = ws;
					bestScore = score;
				}
			}
			result.put(component, bestWorker);
		}
		return result;
	}

	@Override
	public void storeAction(String component, Integer currentDegree, Integer newDegree) {
		try {
			String query = "INSERT INTO scales VALUES('" + this.compMonitor.getTimestamp() + "', '" + component + "', 'scale in', '" + currentDegree + "', '" + newDegree + "')";
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			statement.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store scale in action for component " + component + " because of " + e);
		}
	}

	@Override
	public boolean isGracePeriod(String component) {
		boolean isGrace = false;
		Integer previousTimestamp = this.compMonitor.getTimestamp() - 1;
		Integer oldTimestamp = previousTimestamp - (ComponentMonitor.WINDOW_SIZE * ComponentMonitor.STABIL_COEFF);
		String query = "SELECT * FROM scales WHERE component = '" + component + "' AND timestamp BETWEEN " + oldTimestamp + " AND " + previousTimestamp;
		Statement statement;
		try {
			statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet result = statement.executeQuery(query);
			if(result.next()){
				isGrace = true;
			}
		} catch (SQLException e) {
			logger.severe("Unable to scale component " + component + " because of " + e);
		}
		return isGrace;
	}
}