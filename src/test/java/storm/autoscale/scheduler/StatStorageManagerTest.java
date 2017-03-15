/**
 * 
 */
package storm.autoscale.scheduler;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.Component;
import org.mockito.Mockito;

import junit.framework.TestCase;
import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.connector.database.IJDBCConnector;
import storm.autoscale.scheduler.connector.database.MySQLConnector;
import storm.autoscale.scheduler.modules.StatStorageManager;

/**
 * @author Roland
 *
 */
public class StatStorageManagerTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#storeSpoutExecutorStats(java.lang.Integer, java.lang.String, java.lang.Integer, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Double)}.
	 */
	public void testStoreSpoutExecutorStats() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long totalOutputs = 100L;
			Long totalThroughput = 50L;
			Long totalLosses = 5L;
			Long updateOutputs = 10L;
			Long updateThroughput = 8L;
			Long updateLosses = 0L;
			Double avgLatency = 500.0; 
			manager.storeSpoutExecutorStats(timestamp, host, port, topology, component, startTask, endTask, totalOutputs, updateOutputs, totalThroughput, updateThroughput, totalLosses, updateLosses, avgLatency);
			
			String testSpoutStorageQuery = "SELECT * FROM all_time_spouts_stats";
			ResultSet result = connector.executeQuery(testSpoutStorageQuery);
			
			Integer actualTimestamp = null;
			String actualHost = null;
			Integer actualPort = null;
			String actualTopology = null;
			String actualComponent = null;
			Integer actualStartTask = null;
			Integer actualEndTask = null;
			Long actualTotalOutputs = null;
			Long actualTotalThroughput = null;
			Long actualTotalLosses = null;
			Long actualUpdateOutputs = null;
			Long actualUpdateThroughput = null;
			Long actualUpdateLosses = null;
			Double actualAvgLatency = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualHost = result.getString("host");
				actualPort = result.getInt("port");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualStartTask = result.getInt("start_task");
				actualEndTask = result.getInt("end_task");
				actualTotalOutputs = result.getLong("total_outputs");
				actualTotalThroughput = result.getLong("total_throughput");
				actualTotalLosses = result.getLong("total_losses");
				actualUpdateOutputs = result.getLong("update_outputs");
				actualUpdateThroughput = result.getLong("update_throughput");
				actualUpdateLosses = result.getLong("update_losses");
				actualAvgLatency = result.getDouble("complete_ms_avg");
			}
			assertEquals(timestamp, actualTimestamp, 0);
			assertEquals(host, actualHost);
			assertEquals(port, actualPort, 0);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(startTask, actualStartTask, 0);
			assertEquals(endTask, actualEndTask, 0);
			assertEquals(totalOutputs, actualTotalOutputs, 0);
			assertEquals(totalThroughput, actualTotalThroughput, 0);
			assertEquals(totalLosses, actualTotalLosses, 0);
			assertEquals(updateOutputs, actualUpdateOutputs, 0);
			assertEquals(updateThroughput, actualUpdateThroughput, 0);
			assertEquals(updateLosses, actualUpdateLosses, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store and retrieve spouts logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#storeBoltExecutorStats(java.lang.Integer, java.lang.String, java.lang.Integer, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.Long, java.lang.Long, java.lang.Double, java.lang.Double)}.
	 */
	public void testStoreBoltExecutorStats() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long totalExecuted = 100L;
			Long totalOutputs = 80L;
			Long updateExecuted = 10L;
			Long updateOutputs = 8L;
			Double avgLatency = 500.0;
			Double selectivity = 0.8;
			Double cpuUsage = 40.0;
			manager.storeBoltExecutorStats(timestamp, host, port, topology, component, startTask, endTask, totalExecuted, updateExecuted, totalOutputs, updateOutputs, avgLatency, selectivity, cpuUsage);

			String testBolttStorageQuery = "SELECT * FROM all_time_bolts_stats";
			ResultSet result = connector.executeQuery(testBolttStorageQuery);

			Integer actualTimestamp = null;
			String actualHost = null;
			Integer actualPort = null;
			String actualTopology = null;
			String actualComponent = null;
			Integer actualStartTask = null;
			Integer actualEndTask = null;
			Long actualTotalExecuted = null;
			Long actualTotalOutputs = null;
			Long actualUpdateExecuted = null;
			Long actualUpdateOutputs = null;
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
				actualTotalExecuted = result.getLong("total_executed");
				actualTotalOutputs = result.getLong("total_outputs");
				actualUpdateExecuted = result.getLong("update_executed");
				actualUpdateOutputs = result.getLong("update_outputs");
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
			assertEquals(totalExecuted, actualTotalExecuted, 0);
			assertEquals(totalOutputs, actualTotalOutputs, 0);
			assertEquals(updateExecuted, actualUpdateExecuted, 0);
			assertEquals(updateOutputs, actualUpdateOutputs, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			assertEquals(selectivity, actualSelectivity, 0);

			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store and retrieve bolts logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#storeActivityInfo(java.lang.Integer, java.lang.String, java.lang.String, java.lang.String, java.lang.Double, java.lang.Integer, java.lang.Double)}.
	 */
	public void testStoreActivityInfo() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp = 1;
			String topology = "testTopology";
			String component = "testComponent";
			Double activityValue = 0.85;
			Integer remaining = 50;
			Double capacity = 30.0;
			Double estimatedLoad = 25.0;
	
			manager.storeActivityInfo(timestamp, topology, component, activityValue, remaining, capacity, estimatedLoad);
			
			String testCRStorageQuery = "SELECT * FROM operators_activity";
			ResultSet result = connector.executeQuery(testCRStorageQuery);
			
			Integer actualTimestamp = null;
			String actualTopology = null;
			String actualComponent = null;
			Double actualCRValue = null;
			Integer actualRemaining = null;
			Double actualProcessingRate = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualCRValue = result.getDouble("activity_level");
				actualRemaining = result.getInt("remaining_tuples");
				actualProcessingRate = result.getDouble("capacity_per_second");
			}
			
			assertEquals(timestamp, actualTimestamp);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(activityValue, actualCRValue);
			assertEquals(remaining, actualRemaining);
			assertEquals(capacity, actualProcessingRate);
			
			String testCleanQuery = "DELETE FROM operators_activity";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve activity level logs has failed because of " + e);
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getWorkers(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetWorkers() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			Double cpuUsage1 = 50.0;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			Double cpuUsage2 = 30.0;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage2);
			
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage2);
			
			HashMap<Integer, ArrayList<String>> actualWorkers = manager.getBoltWorkers(component, 11, 10);
			
			ArrayList<String> expectedWorkersTimestamp1 = new ArrayList<>();
			expectedWorkersTimestamp1.add(host1 + "@" + port1);
			expectedWorkersTimestamp1.add(host2 + "@" + port2);
			
			ArrayList<String> expectedWorkersTimestamp2 = new ArrayList<>();
			expectedWorkersTimestamp2.add(host2 + "@" + port2);
			
			HashMap<Integer, ArrayList<String>> expectedWorkers = new HashMap<>();
			expectedWorkers.put(timestamp1, expectedWorkersTimestamp1);
			expectedWorkers.put(timestamp2, expectedWorkersTimestamp2);
			assertEquals(expectedWorkers, actualWorkers);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve worker logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getExecuted(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetExecuted() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			Double cpuUsage1 = 50.0;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			Double cpuUsage2 = 30.0;
			
			Long updateExecuted3 = 12L;
			Long updateExecuted4 = 7L;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted3, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted4, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage2);
			
			HashMap<Integer, Long> actualExecuted = manager.getExecuted(component, 11, 10);
			
			HashMap<Integer, Long> expectedExecuted = new HashMap<>();
			Long expectedExecutedTimestamp1 = updateExecuted1 + updateExecuted2;
			Long expectedExecutedTimestamp2 = updateExecuted3 + updateExecuted4;
			expectedExecuted.put(timestamp1, expectedExecutedTimestamp1);
			expectedExecuted.put(timestamp2, expectedExecutedTimestamp2);
			
			assertEquals(expectedExecuted, actualExecuted);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve execution logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getOutputs(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetOutputs() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			Double cpuUsage1 = 50.0;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			Double cpuUsage2 = 30.0;
			
			Long updateOutputs3 = 100L;
			Long updateOutputs4 = 75L;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage1);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs3, avgLatency1, selectivity1, cpuUsage2);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs4, avgLatency2, selectivity2, cpuUsage2);
			
			HashMap<Integer, Long> actualOutputs = manager.getBoltOutputs(component, 11, 10);
			
			HashMap<Integer, Long> expectedOutputs = new HashMap<>();
			Long expectedOutputsTimestamp1 = updateOutputs1 + updateOutputs2;
			Long expectedOutputsTimestamp2 = updateOutputs3 + updateOutputs4;
			expectedOutputs.put(timestamp1, expectedOutputsTimestamp1);
			expectedOutputs.put(timestamp2, expectedOutputsTimestamp2);
			
			assertEquals(expectedOutputs, actualOutputs);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve emission logs has failed because of " + e);
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getAvgLatency(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetAvgLatency() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			Double cpuUsage1 = 50.0;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			Double cpuUsage2 = 30.0;
			
			Double avgLatency3 = 65.0;
			Double avgLatency4 = 72.0;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage1);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency3, selectivity1, cpuUsage2);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency4, selectivity2, cpuUsage2);
			
			HashMap<Integer, Double> actualAvgLatency = manager.getAvgLatency(component, 11, 10);
			
			HashMap<Integer, Double> expectedAvgLatency = new HashMap<>();
			Double expectedAvgLatencyTimestamp1 = (avgLatency1 + avgLatency2) / 2;
			Double expectedAvgLatencyTimestamp2 = (avgLatency3 + avgLatency4) / 2;
			expectedAvgLatency.put(timestamp1, expectedAvgLatencyTimestamp1);
			expectedAvgLatency.put(timestamp2, expectedAvgLatencyTimestamp2);
			
			assertEquals(expectedAvgLatency, actualAvgLatency);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve component latency logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getSelectivity(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetSelectivity() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			Double cpuUsage1 = 50.0;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			Double cpuUsage2 = 30.0;
			
			Double selectivity3 = 0.76;
			Double selectivity4 = 0.83;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage1);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity3, cpuUsage2);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity4, cpuUsage2);
			
			HashMap<Integer, Double> actualSelectivity = manager.getSelectivity(component, 11, 10);
			
			HashMap<Integer, Double> expectedSelectivity = new HashMap<>();
			Double expectedSelectivityTimestamp1 = new BigDecimal((selectivity1 + selectivity2) / 2).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
			Double expectedSelectivityTimestamp2 = new BigDecimal((selectivity3 + selectivity4) / 2).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
			expectedSelectivity.put(timestamp1, expectedSelectivityTimestamp1);
			expectedSelectivity.put(timestamp2, expectedSelectivityTimestamp2);
			
			assertEquals(expectedSelectivity, actualSelectivity);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve selectivity logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getTopologyThroughput(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyThroughput() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 9;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;
			
			Long updateThroughput3 = 12L;
			Long updateThroughput4 = 14L;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput3, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput4, totalLosses2, updateLosses2, avgLatency2);
			
			HashMap<Integer, Long> actualThroughput = manager.getTopologyThroughput(topology, 11, 10);
			
			HashMap<Integer, Long> expectedThroughput = new HashMap<>();
			Long expectedThroughputTimestamp1 = updateThroughput1 + updateThroughput2;
			Long expectedThroughputTimestamp2 = updateThroughput3 + updateThroughput4;
			expectedThroughput.put(timestamp1, expectedThroughputTimestamp1);
			expectedThroughput.put(timestamp2, expectedThroughputTimestamp2);
			
			assertEquals(expectedThroughput, actualThroughput);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve topology throughput logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getTopologyLosses(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyLosses() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;
			
			Long updateLosses3 = 4L;
			Long updateLosses4 = 5L;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses3, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses4, avgLatency2);
			
			HashMap<Integer, Long> actualLosses = manager.getTopologyLosses(topology, 11, 10);
			
			HashMap<Integer, Long> expectedLosses = new HashMap<>();
			Long expectedLossesTimestamp1 = updateLosses1 + updateLosses2;
			Long expectedLossesTimestamp2 = updateLosses3 + updateLosses4;
			expectedLosses.put(timestamp1, expectedLossesTimestamp1);
			expectedLosses.put(timestamp2, expectedLossesTimestamp2);
			
			assertEquals(expectedLosses, actualLosses);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve failure logs has failed because of " + e);
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.StatStorageManager#getTopologyAvgLatency(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyAvgLatency() {
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;
			
			Double avgLatency3 = 460.0;
			Double avgLatency4 = 332.0;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency3);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency4);
			
			HashMap<Integer, Double> actualAvgLatency = manager.getTopologyAvgLatency(topology, 11, 10);
			
			HashMap<Integer, Double> expectedAvgLatency = new HashMap<>();
			Double expectedAvgLatencyTimestamp1 = Math.max(avgLatency1, avgLatency2);
			Double expectedAvgLatencyTimestamp2 = Math.max(avgLatency3, avgLatency4);
			expectedAvgLatency.put(timestamp1, expectedAvgLatencyTimestamp1);
			expectedAvgLatency.put(timestamp2, expectedAvgLatencyTimestamp2);
			
			assertEquals(expectedAvgLatency, actualAvgLatency);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve topology latency logs because of " + e);
		}
	}

	public void testGetFormerValue(){
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			Integer timestamp3 = 21;
			Integer timestamp4 = 22; 
			String topology = "testTopology";
			String component = "testComponent";

			String host1 = "testHost1";
			Integer port = 0;
			Integer startTask = 0;
			Integer endTask = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;


			String host2 = "testHost2";
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;

			Long totalOutputs3 = 150L;
			Long totalThroughput3 = 90L;
			Long totalLosses3 = 15L;
			Long updateOutputs3 = 15L;
			Long updateThroughput3 = 9L;
			Long updateLosses3 = 1L;
			Double avgLatency3 = 700.0;

			manager.storeSpoutExecutorStats(timestamp1, host1, port, topology, component, startTask, endTask, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host1, port, topology, component, startTask, endTask, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			manager.storeSpoutExecutorStats(timestamp3, host2, port, topology, component, startTask, endTask, totalOutputs3, updateOutputs3, totalThroughput3, updateThroughput3, totalLosses3, updateLosses3, avgLatency3);

			Long actual1 = manager.getFormerValue(component, startTask, endTask, timestamp1, "spout", "total_outputs");
			Long actual2 = manager.getFormerValue(component, startTask, endTask, timestamp2, "spout", "total_outputs");
			Long actual3 = manager.getFormerValue(component, startTask, endTask, timestamp3, "spout", "update_outputs");
			Long actual4 = manager.getFormerValue(component, startTask, endTask, timestamp4, "spout", "update_outputs");
			Long actual5 = manager.getFormerValue(component, startTask, endTask, timestamp4, "spout", "update_losses");

			assertEquals(0L, actual1, 0);
			assertEquals(100L, actual2, 0);
			assertEquals(10L, actual3, 0);
			assertEquals(15L, actual4, 0);
			assertEquals(1L, actual5, 0);

			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve historical logs because of " + e);
		}
	}
	
	public void testStoreTopologyConstraints(){
		try{
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			
			TopologyDetails topology = Mockito.mock(TopologyDetails.class);
			Mockito.when(topology.getName()).thenReturn("topologyTest");
			
			Component compA = new Component("A");
			Component compB = new Component("B");
			Component compC = new Component("C");
			Component compD = new Component("D");
			
			ExecutorDetails execA1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execB1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execC1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execC2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execD1 = Mockito.mock(ExecutorDetails.class);
			
			ArrayList<ExecutorDetails> execsA = new ArrayList<>();
			execsA.add(execA1);
			execsA.add(execA2);
			ArrayList<ExecutorDetails> execsB = new ArrayList<>();
			execsB.add(execB1);
			ArrayList<ExecutorDetails> execsC = new ArrayList<>();
			execsC.add(execC1);
			execsC.add(execC2);
			ArrayList<ExecutorDetails> execsD = new ArrayList<>();
			execsD.add(execD1);
			
			compA.execs = execsA;
			compB.execs = execsB;
			compC.execs = execsC;
			compD.execs = execsD;
			
			HashMap<String, Component> components = new HashMap<>();
			components.put("A", compA);
			components.put("B", compB);
			components.put("C", compC);
			components.put("D", compD);
			
			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execB1)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execC1)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execC2)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execD1)).thenReturn(40.0);
			

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execB1)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execC1)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execC2)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execD1)).thenReturn(256.0);
			
			manager.storeTopologyConstraints(0, topology);
			
			String testTopologyConstraintsQueryA = "SELECT * FROM operators_constraints WHERE component = 'A'";
			ResultSet resultA = connector.executeQuery(testTopologyConstraintsQueryA);
			Integer timestampA = null;
			String topologyA = null;
			String componentA = null;
			String typeA = null;
			Double cpuA = null;
			Double memA = null;
			
			while(resultA.next()){
				timestampA = resultA.getInt("timestamp");
				topologyA = resultA.getString("topology");
				componentA = resultA.getString("component");
				typeA = resultA.getString("type");
				cpuA = resultA.getDouble("cpu");
				memA = resultA.getDouble("memory");
			}
			
			assertEquals(0, timestampA, 0);
			assertEquals("topologyTest", topologyA);
			assertEquals("A", componentA);
			assertEquals("initial", typeA);
			assertEquals(10.0, cpuA, 0.0);
			assertEquals(32.0, memA, 0.0);
			
			String testTopologyConstraintsQueryB = "SELECT * FROM operators_constraints WHERE component = 'B'";
			ResultSet resultB = connector.executeQuery(testTopologyConstraintsQueryB);
			Integer timestampB = null;
			String topologyB = null;
			String componentB = null;
			String typeB = null;
			Double cpuB = null;
			Double memB = null;
			
			while(resultB.next()){
				timestampB = resultB.getInt("timestamp");
				topologyB = resultB.getString("topology");
				componentB = resultB.getString("component");
				typeB = resultB.getString("type");
				cpuB = resultB.getDouble("cpu");
				memB = resultB.getDouble("memory");
			}
			
			assertEquals(0, timestampB, 0);
			assertEquals("topologyTest", topologyB);
			assertEquals("B", componentB);
			assertEquals("initial", typeB);
			assertEquals(20.0, cpuB, 0.0);
			assertEquals(64.0, memB, 0.0);
			
			String testTopologyConstraintsQueryC = "SELECT * FROM operators_constraints WHERE component = 'C'";
			ResultSet resultC = connector.executeQuery(testTopologyConstraintsQueryC);
			Integer timestampC = null;
			String topologyC = null;
			String componentC = null;
			String typeC = null;
			Double cpuC = null;
			Double memC = null;
			
			while(resultC.next()){
				timestampC = resultC.getInt("timestamp");
				topologyC = resultC.getString("topology");
				componentC = resultC.getString("component");
				typeC = resultC.getString("type");
				cpuC = resultC.getDouble("cpu");
				memC = resultC.getDouble("memory");
			}
			
			assertEquals(0, timestampC, 0);
			assertEquals("topologyTest", topologyC);
			assertEquals("C", componentC);
			assertEquals("initial", typeC);
			assertEquals(30.0, cpuC, 0.0);
			assertEquals(128.0, memC, 0.0);
			
			String testTopologyConstraintsQueryD = "SELECT * FROM operators_constraints WHERE component = 'D'";
			ResultSet resultD = connector.executeQuery(testTopologyConstraintsQueryD);
			Integer timestampD = null;
			String topologyD = null;
			String componentD = null;
			String typeD = null;
			Double cpuD = null;
			Double memD = null;
			
			while(resultD.next()){
				timestampD = resultD.getInt("timestamp");
				topologyD = resultD.getString("topology");
				componentD = resultD.getString("component");
				typeD = resultD.getString("type");
				cpuD = resultD.getDouble("cpu");
				memD = resultD.getDouble("memory");
			}
			
			assertEquals(0, timestampD, 0);
			assertEquals("topologyTest", topologyD);
			assertEquals("D", componentD);
			assertEquals("initial", typeD);
			assertEquals(40.0, cpuD, 0.0);
			assertEquals(256.0, memD, 0.0);
			
			String testCleanQuery = "DELETE FROM operators_constraints";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store topology constraints because of " + e);
		}
	}
	
	public void testIsInitialConstraint(){
		try{
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			
			TopologyDetails topology = Mockito.mock(TopologyDetails.class);
			Mockito.when(topology.getName()).thenReturn("topologyTest");
			
			Component compA = new Component("A");
			
			ExecutorDetails execA1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA3 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA4 = Mockito.mock(ExecutorDetails.class);
			
			ArrayList<ExecutorDetails> execsA = new ArrayList<>();
			execsA.add(execA1);
			execsA.add(execA2);
			execsA.add(execA3);
			execsA.add(execA4);
			
			compA.execs = execsA;
			
			HashMap<String, Component> components = new HashMap<>();
			components.put("A", compA);
			
			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(10.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(32.0);
			
			manager.storeTopologyConstraints(0, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(20.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(64.0);
			
			manager.storeTopologyConstraints(1, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(30.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(128.0);
			
			manager.storeTopologyConstraints(2, topology);
			
			assertEquals(true, manager.isInitialConstraint(0, "topologyTest", "A"));
			assertEquals(true, manager.isInitialConstraint(0, "nottopologyTest", "A"));
			assertEquals(false, manager.isInitialConstraint(1, "topologyTest", "A"));
			assertEquals(false, manager.isInitialConstraint(2, "topologyTest", "A"));
			assertEquals(true, manager.isInitialConstraint(0, "topologyTest", "B"));
			
			String testCleanQuery = "DELETE FROM operators_constraints";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store topology constraints because of " + e);
		}
	}
	
	public void testGetInitialCpuConstraint(){
		try{
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			
			TopologyDetails topology = Mockito.mock(TopologyDetails.class);
			Mockito.when(topology.getName()).thenReturn("topologyTest");
			
			Component compA = new Component("A");
			
			ExecutorDetails execA1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA3 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA4 = Mockito.mock(ExecutorDetails.class);
			
			ArrayList<ExecutorDetails> execsA = new ArrayList<>();
			execsA.add(execA1);
			execsA.add(execA2);
			execsA.add(execA3);
			execsA.add(execA4);
			
			compA.execs = execsA;
			
			HashMap<String, Component> components = new HashMap<>();
			components.put("A", compA);
			
			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(10.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(32.0);
			
			manager.storeTopologyConstraints(0, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(20.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(64.0);
			
			manager.storeTopologyConstraints(1, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(30.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(128.0);
			
			manager.storeTopologyConstraints(2, topology);
			
			assertEquals(10.0, manager.getInitialCpuConstraint("topologyTest", "A"));
			assertEquals(0.0, manager.getInitialCpuConstraint("nottopologyTest", "A"));
			assertEquals(0.0, manager.getInitialCpuConstraint("topologyTest", "B"));
			
			String testCleanQuery = "DELETE FROM operators_constraints";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store topology constraints because of " + e);
		}
	}
	
	public void testGetInitialMemConstraint(){
		try{
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			
			TopologyDetails topology = Mockito.mock(TopologyDetails.class);
			Mockito.when(topology.getName()).thenReturn("topologyTest");
			
			Component compA = new Component("A");
			
			ExecutorDetails execA1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA3 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA4 = Mockito.mock(ExecutorDetails.class);
			
			ArrayList<ExecutorDetails> execsA = new ArrayList<>();
			execsA.add(execA1);
			execsA.add(execA2);
			execsA.add(execA3);
			execsA.add(execA4);
			
			compA.execs = execsA;
			
			HashMap<String, Component> components = new HashMap<>();
			components.put("A", compA);
			
			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(10.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(32.0);
			
			manager.storeTopologyConstraints(0, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(20.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(64.0);
			
			manager.storeTopologyConstraints(1, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(30.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(128.0);
			
			manager.storeTopologyConstraints(2, topology);
			
			assertEquals(32.0, manager.getInitialMemConstraint("topologyTest", "A"));
			assertEquals(0.0, manager.getInitialMemConstraint("nottopologyTest", "A"));
			assertEquals(0.0, manager.getInitialMemConstraint("topologyTest", "B"));
			
			String testCleanQuery = "DELETE FROM operators_constraints";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store topology constraints because of " + e);
		}
	}
	
	public void testGetCurrentCpuConstraint(){
		try{
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			
			TopologyDetails topology = Mockito.mock(TopologyDetails.class);
			Mockito.when(topology.getName()).thenReturn("topologyTest");
			
			Component compA = new Component("A");
			
			ExecutorDetails execA1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA3 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA4 = Mockito.mock(ExecutorDetails.class);
			
			ArrayList<ExecutorDetails> execsA = new ArrayList<>();
			execsA.add(execA1);
			execsA.add(execA2);
			execsA.add(execA3);
			execsA.add(execA4);
			
			compA.execs = execsA;
			
			HashMap<String, Component> components = new HashMap<>();
			components.put("A", compA);
			
			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(10.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(32.0);
			
			manager.storeTopologyConstraints(0, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(20.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(64.0);
			
			manager.storeTopologyConstraints(1, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(30.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(128.0);
			
			manager.storeTopologyConstraints(2, topology);
			
			assertEquals(30.0, manager.getCurrentCpuConstraint("topologyTest", "A"));
			assertEquals(0.0, manager.getCurrentCpuConstraint("nottopologyTest", "A"));
			assertEquals(0.0, manager.getCurrentCpuConstraint("topologyTest", "B"));
			
			String testCleanQuery = "DELETE FROM operators_constraints";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store topology constraints because of " + e);
		}
	}
	
	public void testGetCurrentMemConstraint(){
		try{
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			
			TopologyDetails topology = Mockito.mock(TopologyDetails.class);
			Mockito.when(topology.getName()).thenReturn("topologyTest");
			
			Component compA = new Component("A");
			
			ExecutorDetails execA1 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA2 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA3 = Mockito.mock(ExecutorDetails.class);
			ExecutorDetails execA4 = Mockito.mock(ExecutorDetails.class);
			
			ArrayList<ExecutorDetails> execsA = new ArrayList<>();
			execsA.add(execA1);
			execsA.add(execA2);
			execsA.add(execA3);
			execsA.add(execA4);
			
			compA.execs = execsA;
			
			HashMap<String, Component> components = new HashMap<>();
			components.put("A", compA);
			
			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(10.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(10.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(32.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(32.0);
			
			manager.storeTopologyConstraints(0, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(20.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(20.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(64.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(64.0);
			
			manager.storeTopologyConstraints(1, topology);

			Mockito.when(topology.getComponents()).thenReturn(components);
			Mockito.when(topology.getTotalCpuReqTask(execA1)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA2)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA3)).thenReturn(30.0);
			Mockito.when(topology.getTotalCpuReqTask(execA4)).thenReturn(30.0);

			Mockito.when(topology.getTotalMemReqTask(execA1)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA2)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA3)).thenReturn(128.0);
			Mockito.when(topology.getTotalMemReqTask(execA4)).thenReturn(128.0);
			
			manager.storeTopologyConstraints(2, topology);
			
			assertEquals(128.0, manager.getCurrentMemConstraint("topologyTest", "A"));
			assertEquals(0.0, manager.getCurrentMemConstraint("nottopologyTest", "A"));
			assertEquals(0.0, manager.getCurrentMemConstraint("topologyTest", "B"));
			
			String testCleanQuery = "DELETE FROM operators_constraints";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to store topology constraints because of " + e);
		}
	}
	
	public void testGetCpuUsage(){
		try {
			XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
			Mockito.when(parser.getDbHost()).thenReturn("localhost");
			Mockito.when(parser.getDbName()).thenReturn("autoscale_test");
			Mockito.when(parser.getDbUser()).thenReturn("root");
			Mockito.when(parser.getDbPassword()).thenReturn("");
			
			IJDBCConnector connector = new MySQLConnector(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			StatStorageManager manager = StatStorageManager.getManager(parser.getDbHost(), parser.getDbName(), parser.getDbUser(), parser.getDbPassword());
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			Integer timestamp3 = 30;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			Double cpuUsage1 = 50.0;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			Double cpuUsage2 = 30.0;
			
			Double cpuUsage3 = 60.0;
			Double cpuUsage4 = 25.0;
			
			Double cpuUsage5 = 55.0;
			Double cpuUsage6 = 32.0;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage3);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage4);
			
			manager.storeBoltExecutorStats(timestamp3, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1, cpuUsage5);
			manager.storeBoltExecutorStats(timestamp3, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2, cpuUsage6);
			
			HashMap<Integer, ArrayList<Double>> cpuUsage = new HashMap<>();
			ArrayList<Double> usages1 = new ArrayList<>();
			usages1.add(50.0);
			usages1.add(30.0);
			
			ArrayList<Double> usages2 = new ArrayList<>();
			usages2.add(60.0);
			usages2.add(25.0);
			
			ArrayList<Double> usages3 = new ArrayList<>();
			usages3.add(55.0);
			usages3.add(32.0);
			
			cpuUsage.put(timestamp1, usages1);
			cpuUsage.put(timestamp2, usages2);
			cpuUsage.put(timestamp3, usages3);
			
			assertEquals(cpuUsage, manager.getCpuUsage(component, timestamp3, 60));
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			connector.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			fail("StatStorageManager module has failed to retrieve component latency logs has failed because of " + e);
		}
	}
}