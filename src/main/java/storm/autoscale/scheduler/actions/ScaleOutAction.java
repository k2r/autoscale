/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.allocation.IAllocationStrategy;
import storm.autoscale.scheduler.connector.database.IJDBCConnector;
import storm.autoscale.scheduler.connector.database.MySQLConnector;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ScaleOutAction implements IAction {

	private ComponentMonitor cm;
	private TopologyExplorer explorer;
	private AssignmentMonitor am;
	private IAllocationStrategy allocStrategy;
	private String nimbusHost;
	private Integer nimbusPort;
	private IJDBCConnector connector;
	private Set<String> validateActions;
	private Logger logger = Logger.getLogger("ScaleOutAction");
	
	public ScaleOutAction(ComponentMonitor compMonitor, TopologyExplorer explorer,
			AssignmentMonitor assignMonitor, IAllocationStrategy allocStrategy, String nimbusHost,
			Integer nimbusPort) {
		this.cm = compMonitor;
		this.explorer = explorer;
		this.am = assignMonitor;
		this.allocStrategy = allocStrategy;
		this.nimbusHost = nimbusHost;
		this.nimbusPort = nimbusPort;
		ArrayList<String> actions = this.cm.getScaleOutRequirements();
		this.validateActions = new HashSet<>();
		for(String action : actions){
			this.validateActions.add(action);
		}
		try {
			String host = this.cm.getParser().getDbHost();
			String name = this.cm.getParser().getDbName();
			String user = this.cm.getParser().getDbUser();
			String pwd = this.cm.getParser().getDbPassword();
			this.connector = new MySQLConnector(host, name, user, pwd);
		} catch (ClassNotFoundException | SQLException e) {
			logger.severe("Unable to perform the scale-out action because " + e);
		}
		Thread thread = new Thread(this);
		thread.start();
	}
	
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
				Double crValue = this.cm.getActivityValue(component);
				int maxParallelism = this.am.getAllSortedTasks(component).size();

				int currentParallelism = this.am.getParallelism(component);
				int estimatedParallelism  = (int) Math.round(crValue * currentParallelism);
		
				int newParallelism = Math.min(maxParallelism, estimatedParallelism);
				if(newParallelism > currentParallelism && !isGracePeriod(component)){
					RebalanceOptions options = new RebalanceOptions();
					options.put_to_num_executors(component, newParallelism);
					options.set_num_workers(this.am.getNbWorkers());
					options.set_wait_secs(0);

					logger.fine("Changing parallelism degree of component " + component + " from " + currentParallelism + " to " + newParallelism + "...");

					client.rebalance(explorer.getTopologyName(), options);
					logger.fine("Parallelism of component " + component + " increased successfully!");
					storeAction(component, currentParallelism, newParallelism);
				}else{
					logger.fine("This scale-out will not improve the distribution of the operator");
				}
				Thread.sleep(1000);
			}
		} catch (TException | InterruptedException e) {
			logger.severe("Unable to scale topology " + explorer.getTopologyName() + " because of " + e);
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
			String query = "INSERT INTO scales VALUES('" + this.cm.getTimestamp() + "', '" + component + "', 'scale out', '" + currentDegree + "', '" + newDegree + "')";
			this.connector.executeUpdate(query);
		} catch(SQLException e) {
			logger.severe("Unable to store the scale out action for component " + component +  " because " + e);
		}
	}

	@Override
	public boolean isGracePeriod(String component) {
		boolean isGrace = false;
		Integer previousTimestamp = this.cm.getTimestamp() - 1;
		Integer oldTimestamp = (int) (previousTimestamp - Math.round((this.cm.getParser().getWindowSize() * this.cm.getParser().getStabilizationCoeff())));
		String query = "SELECT * FROM scales WHERE component = '" + component + "' AND timestamp BETWEEN " + oldTimestamp + " AND " + previousTimestamp;
		try {
			ResultSet result = this.connector.executeQuery(query);
			if(result.next()){
				isGrace = true;
			}
		} catch (SQLException e) {
			logger.severe("Unable to scale component " + component + " because of " + e);
		}
		return isGrace;
	}
	
}