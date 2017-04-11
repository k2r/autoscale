/**
 * 
 */
package storm.autoscale.scheduler.action;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;

import storm.autoscale.scheduler.connector.database.IJDBCConnector;
import storm.autoscale.scheduler.connector.database.MySQLConnector;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
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
	private TopologyExplorer explorer;
	private Double nbWorkers;
	
	private IJDBCConnector connector;
	
	private Logger logger = Logger.getLogger("ScaleActionTrigger");
	
	public ScaleActionTrigger(String nimbusHost, Integer nimbusPort, ComponentMonitor cm, ScalingManager3 sm, TopologyExplorer explorer, Double nbWorkers){
		this.nimbusHost = nimbusHost;
		this.nimbusPort = nimbusPort;
		this.cm = cm;
		this.sm = sm;
		this.explorer = explorer;
		this.nbWorkers = nbWorkers;
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
	 * @see storm.autoscale.scheduler.action.IActionTrigger#storeAction(java.lang.String, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public void storeAction(String component, Integer currentDegree, Integer newDegree) {
		try {
			String actionType = "";
			if(currentDegree < newDegree){
				actionType += "scale-in";
			}else{
				actionType += "scale-out";
			}
			
			String query = "INSERT INTO scales VALUES('" + this.cm.getTimestamp() + "', '" + component + "', '" + actionType + "', '" + currentDegree + "', '" + newDegree + "')";
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
		
	}

}