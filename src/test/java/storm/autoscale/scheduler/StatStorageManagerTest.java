/**
 * 
 */
package storm.autoscale.scheduler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class StatStorageManagerTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#storeSpoutExecutorStats(java.lang.Integer, java.lang.String, java.lang.Integer, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Double)}.
	 */
	public void testStoreSpoutExecutorStats() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long outputs = 100L;
			Long throughput = 50L;
			Long losses = 5L;
			Double avgLatency = 500.0; 
			manager.storeSpoutExecutorStats(timestamp, host, port, topology, component, startTask, endTask, outputs, throughput, losses, avgLatency);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			String testSpoutStorageQuery = "SELECT * FROM all_time_spouts_stats";
			ResultSet result = statement.executeQuery(testSpoutStorageQuery);
			
			Integer actualTimestamp = null;
			String actualHost = null;
			Integer actualPort = null;
			String actualTopology = null;
			String actualComponent = null;
			Integer actualStartTask = null;
			Integer actualEndTask = null;
			Long actualOutputs = null;
			Long actualThroughput = null;
			Long actualLosses = null;
			Double actualAvgLatency = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualHost = result.getString("host");
				actualPort = result.getInt("port");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualStartTask = result.getInt("start_task");
				actualEndTask = result.getInt("end_task");
				actualOutputs = result.getLong("outputs");
				actualThroughput = result.getLong("throughput");
				actualLosses = result.getLong("losses");
				actualAvgLatency = result.getDouble("complete_ms_avg");
			}
			assertEquals(timestamp, actualTimestamp, 0);
			assertEquals(host, actualHost);
			assertEquals(port, actualPort, 0);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(startTask, actualStartTask, 0);
			assertEquals(endTask, actualEndTask, 0);
			assertEquals(outputs, actualOutputs, 0);
			assertEquals(throughput, actualThroughput, 0);
			assertEquals(losses, actualLosses, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#storeBoltExecutorStats(java.lang.Integer, java.lang.String, java.lang.Integer, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.Long, java.lang.Long, java.lang.Double, java.lang.Double)}.
	 */
	public void testStoreBoltExecutorStats() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long executed = 100L;
			Long outputs = 80L;
			Double avgLatency = 500.0;
			Double selectivity = 0.8;
			manager.storeBoltExecutorStats(timestamp, host, port, topology, component, startTask, endTask, executed, outputs, avgLatency, selectivity);

			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);

			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			String testBolttStorageQuery = "SELECT * FROM all_time_bolts_stats";
			ResultSet result = statement.executeQuery(testBolttStorageQuery);

			Integer actualTimestamp = null;
			String actualHost = null;
			Integer actualPort = null;
			String actualTopology = null;
			String actualComponent = null;
			Integer actualStartTask = null;
			Integer actualEndTask = null;
			Long actualExecuted = null;
			Long actualOutputs = null;
			Double actualAvgLatency = null;
			Double actualSelectivity = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualHost = result.getString("host");
				actualPort = result.getInt("port");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualStartTask = result.getInt("start_task");
				actualEndTask = result.getInt("end_task");
				actualExecuted = result.getLong("executed");
				actualOutputs = result.getLong("outputs");
				actualAvgLatency = result.getDouble("execute_ms_avg");
				actualSelectivity = result.getDouble("selectivity");
			}
			assertEquals(timestamp, actualTimestamp, 0);
			assertEquals(host, actualHost);
			assertEquals(port, actualPort, 0);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(startTask, actualStartTask, 0);
			assertEquals(endTask, actualEndTask, 0);
			assertEquals(executed, actualExecuted, 0);
			assertEquals(outputs, actualOutputs, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			assertEquals(selectivity, actualSelectivity, 0);

			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getWorkers(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetWorkers() {
		try {
			
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			ArrayList<String> actualWorkers = manager.getWorkers(component, timestamp);
			
			ArrayList<String> expectedWorkers = new ArrayList<>();
			expectedWorkers.add(host1 + "@" + port1);
			expectedWorkers.add(host2 + "@" + port2);
			
			assertEquals(expectedWorkers, actualWorkers);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getExecuted(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetExecuted() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			Long actualExecuted = manager.getExecuted(component, timestamp);
			Long expectedExecuted = executed1 + executed2;
			
			assertEquals(expectedExecuted, actualExecuted, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getOutputs(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetOutputs() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			Long actualOutputs = manager.getOutputs(component, timestamp);
			Long expectedOutputs = outputs1 + outputs2;
			
			assertEquals(expectedOutputs, actualOutputs, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getAvgLatency(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetAvgLatency() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			Double actualAvgLatency = manager.getAvgLatency(component, timestamp);
			Double expectedAvgLatency = (avgLatency1 + avgLatency2) / 2;
			
			assertEquals(expectedAvgLatency, actualAvgLatency, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getSelectivity(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetSelectivity() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			Double actualSelectivity = manager.getSelectivity(component, timestamp);
			Double expectedSelectivity = (selectivity1 + selectivity2) / 2;
			
			assertEquals(expectedSelectivity, actualSelectivity, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getTopologyThroughput(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyThroughput() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long outputs1 = 100L;
			Long throughput1 = 75L;
			Long losses1 = 10L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long outputs2 = 120L;
			Long throughput2 = 90L;
			Long losses2 = 15L;
			Double avgLatency2 = 700.0;
			
			manager.storeSpoutExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency2);
			
			Long actualThroughput = manager.getTopologyThroughput(topology, timestamp);
			Long expectedThroughput = throughput1 + throughput2;
			
			assertEquals(expectedThroughput, actualThroughput, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getTopologyLosses(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyLosses() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long outputs1 = 100L;
			Long throughput1 = 75L;
			Long losses1 = 10L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long outputs2 = 120L;
			Long throughput2 = 90L;
			Long losses2 = 15L;
			Double avgLatency2 = 700.0;
			
			manager.storeSpoutExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency2);
			
			Long actualLosses = manager.getTopologyLosses(topology, timestamp);
			Long expectedLosses = losses1 + losses2;
			
			assertEquals(expectedLosses, actualLosses, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getTopologyAvgLatency(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyAvgLatency() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long outputs1 = 100L;
			Long throughput1 = 75L;
			Long losses1 = 10L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long outputs2 = 120L;
			Long throughput2 = 90L;
			Long losses2 = 15L;
			Double avgLatency2 = 700.0;
			
			manager.storeSpoutExecutorStats(timestamp, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency2);
			
			Double actualAvgLatency = manager.getTopologyAvgLatency(topology, timestamp);
			Double expectedAvgLatency = Math.max(avgLatency1, avgLatency2);
			
			assertEquals(expectedAvgLatency, actualAvgLatency, 0);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

}
