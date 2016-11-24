/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import storm.autoscale.scheduler.connector.database.IJDBCConnector;
import storm.autoscale.scheduler.connector.database.MySQLConnector;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ScaleInAction implements IAction {

	private ComponentMonitor cm;
	private TopologyExplorer explorer;
	private AssignmentMonitor am;
	private String nimbusHost;
	private Integer nimbusPort;
	private IJDBCConnector connector;
	
	private HashMap<String, Integer> actions;
	private Logger logger = Logger.getLogger("ScaleInAction");
	
	/**
	 * 
	 */
	public ScaleInAction(ComponentMonitor compMonitor, TopologyExplorer explorer,
			AssignmentMonitor assignMonitor, String nimbusHost, Integer nimbusPort) {
		this.cm = compMonitor;
		this.explorer = explorer;
		this.am = assignMonitor;
		this.nimbusHost = nimbusHost;
		this.nimbusPort = nimbusPort;
		this.actions = this.cm.getScaleInActions();
		try {
			String host = this.cm.getParser().getDbHost();
			String name = this.cm.getParser().getDbName();
			String user = this.cm.getParser().getDbUser();
			String pwd = this.cm.getParser().getDbPassword();
			this.connector = new MySQLConnector(host, name, user, pwd);
		} catch (ClassNotFoundException | SQLException e) {
			logger.severe("Unable to perform the scale-in action because " + e);
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
			for(String component : this.actions.keySet()){
				
				Integer currentDegree = this.cm.getCurrentDegree(component);
				Integer adequateDegree = this.actions.get(component);
				
				if(!isGracePeriod(component)){
					
					RebalanceOptions options = new RebalanceOptions();
					options.put_to_num_executors(component, adequateDegree);
					options.set_num_workers(this.am.getNbWorkers());
					options.set_wait_secs(0);

					logger.fine("Changing parallelism degree of component " + component + " from " + currentDegree + " to " + adequateDegree + "...");

					client.rebalance(explorer.getTopologyName(), options);
					logger.fine("Parallelism of component " + component + " decreased successfully!");
					storeAction(component, currentDegree, adequateDegree);
				}else{
					logger.fine("This scale-in will not decrease the distribution of the operator");
				}
				Thread.sleep(1000);
			}
		}catch (TException | InterruptedException e) {
			logger.fine("Unable to scale topology " + explorer.getTopologyName() + " because of " + e);
		}
	}

	@Override
	public void storeAction(String component, Integer currentDegree, Integer adequateDegree) {
		try {
			String query = "INSERT INTO scales VALUES('" + this.cm.getTimestamp() + "', '" + component + "', 'scale in', '" + currentDegree + "', '" + adequateDegree + "')";
			this.connector.executeUpdate(query);
		} catch (SQLException e) {
			logger.severe("Unable to store scale in action for component " + component + " because of " + e);
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