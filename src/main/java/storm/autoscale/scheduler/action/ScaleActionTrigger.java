/**
 * 
 */
package storm.autoscale.scheduler.action;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;

import storm.autoscale.scheduler.connector.database.IJDBCConnector;
import storm.autoscale.scheduler.connector.database.MySQLConnector;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.scale.ScalingManager3;

/**
 * @author Roland
 *
 */
public class ScaleActionTrigger implements IActionTrigger {
	
	private String nimbusHost;
	private Integer nimbusPort;
	
	private ScalingManager3 sm;
	private ComponentMonitor cm;
	private Integer nbWorkers;
	private TopologyDetails topology;
	private IJDBCConnector connector;
	
	private Logger logger = Logger.getLogger("ScaleActionTrigger");
	
	public ScaleActionTrigger(String nimbusHost, Integer nimbusPort, ComponentMonitor cm, ScalingManager3 sm, Integer nbWorkers, TopologyDetails topology){
		this.nimbusHost = nimbusHost;
		this.nimbusPort = nimbusPort;
		this.cm = cm;
		this.sm = sm;
		this.nbWorkers = nbWorkers;
		this.topology = topology;
		try {
			String host = this.cm.getParser().getDbHost();
			String name = this.cm.getParser().getDbName();
			String user = this.cm.getParser().getDbUser();
			String pwd = this.cm.getParser().getDbPassword();
			this.connector = new MySQLConnector(host, name, user, pwd);
		} catch (ClassNotFoundException | SQLException e) {
			logger.severe("Unable to trigger a scaling action because " + e);
		}
 		Thread thread = new Thread(this);
		thread.start();
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.action.IActionTrigger#storeAction(java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.String)
	 */
	@Override
	public void storeAction(String component, Integer currentDegree, Integer newDegree, String actionType) {
		try {
			String query = "INSERT INTO scales VALUES('" + this.cm.getTimestamp() + "', '" + this.topology.getId() + "', '" + component + "', '" + actionType + "', '" + currentDegree + "', '" + newDegree + "')";
			this.connector.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store scale in action for component " + component + " because of " + e);
		}
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.action.IActionTrigger#isGracePeriod(java.lang.String)
	 */
	@Override
	public boolean isGracePeriod(String component) {
		boolean isGrace = false;
		Integer previousTimestamp = this.cm.getTimestamp() - 1;
		Integer oldTimestamp = (int) (previousTimestamp - Math.round((this.cm.getParser().getWindowSize() * this.cm.getParser().getGraceCoeff())));
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

	public void submitRebalance(Nimbus.Client client, String component, Integer curr, Integer next, Integer nbWorkers, String actionType){
		try {
			if(!isGracePeriod(component)){
				RebalanceOptions options = new RebalanceOptions();
				options.put_to_num_executors(component, next);
				options.set_num_workers(nbWorkers);
				options.set_wait_secs(0);

				logger.fine("Changing parallelism degree of component " + component + " from " + curr + " to " + next + "...");

				client.rebalance(this.topology.getName(), options);

				logger.fine("Parallelism of component " + component + " increased successfully!");
				storeAction(component, curr, next, actionType);
				Thread.sleep(1000);
			}else{
				logger.fine("This rebalance action will not modify the distribution of the operator");
			}
		} catch (TException | InterruptedException e) {
			logger.severe("Unable to scale topology " + this.topology.getId() + " because of " + e);
		}
	}
	
	@Override
	public void run() {
		TSocket tsocket = new TSocket(this.nimbusHost, this.nimbusPort);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		try {
			if(!tTransport.isOpen()){
				tTransport.open();
			}
			HashMap<String, Integer> scaleOutActions = this.sm.getScaleOutActions();
			HashMap<String, Integer> scaleInActions = this.sm.getScaleInActions();
			
			for(String component : scaleOutActions.keySet()){
				Integer curr = this.sm.getDegree(component);
				Integer next = scaleOutActions.get(component);
				submitRebalance(client, component, curr, next, this.nbWorkers, "scale-out");
				
			}
			for(String component : scaleInActions.keySet()){
				Integer curr = this.sm.getDegree(component);
				Integer next = scaleInActions.get(component);
				submitRebalance(client, component, curr, next, this.nbWorkers, "scale-in");
			
			}
		}catch(TException e){
			logger.severe("Unable to submit rebalance because of " + e);
		}
	}

}