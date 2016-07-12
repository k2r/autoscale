/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ComponentWindowedStatsTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentWindowedStats#getRecordedTimestamps(java.util.HashMap)}.
	 */
	public void testGetRecordedTimestamps() {
		HashMap<Integer, Long> records = new HashMap<>();
		records.put(1, 500L);
		records.put(2, 600L);
		records.put(3, 675L);
		records.put(4, 550L);
		records.put(5, 475L);
		records.put(6, 390L);
		
		ArrayList<Integer> actual = ComponentWindowedStats.getRecordedTimestamps(records);
		
		ArrayList<Integer> expected = new ArrayList<>();
		expected.add(6);
		expected.add(5);
		expected.add(4);
		expected.add(3);
		expected.add(2);
		expected.add(1);
		
		assertEquals(expected, actual);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentWindowedStats#getRecord(java.util.HashMap, java.lang.Integer)}.
	 */
	public void testGetRecord() {
		HashMap<Integer, Long> records1 = new HashMap<>();
		records1.put(1, 500L);
		records1.put(2, 600L);
		records1.put(3, 675L);
		records1.put(4, 550L);
		records1.put(5, 475L);
		records1.put(6, 390L);
		
		Double actual1 = ComponentWindowedStats.getRecord(records1, 0);
		Double actual2 = ComponentWindowedStats.getRecord(records1, 2);
		Double actual3 = ComponentWindowedStats.getRecord(records1, 5);
		
		HashMap<Integer, Double> records2 = new HashMap<>();
		records2.put(1, 500.0);
		records2.put(2, 600.0);
		records2.put(3, 390.0);
		records2.put(4, 550.0);
		records2.put(5, 475.0);
		records2.put(6, 390.0);
		
		Double actual4 = ComponentWindowedStats.getRecord(records2, 0);
		Double actual5 = ComponentWindowedStats.getRecord(records2, 2);
		Double actual6 = ComponentWindowedStats.getRecord(records2, 5);
		
		HashMap<Integer, Long> records3 = new HashMap<>();
		
		Double actual7 = ComponentWindowedStats.getRecord(records3, 0);
		Double actual8 = ComponentWindowedStats.getRecord(records3, 2);
		Double actual9 = ComponentWindowedStats.getRecord(records3, 5);
		Double actual10 = ComponentWindowedStats.getRecord(records3, 128);
		
		assertEquals(390.0, actual1, 0);
		assertEquals(550.0, actual2, 0);
		assertEquals(500.0, actual3, 0);
		assertEquals(390.0, actual4, 0);
		assertEquals(550.0, actual5, 0);
		assertEquals(500.0, actual6, 0);
		assertEquals(0.0, actual7, 0);
		assertEquals(0.0, actual8, 0);
		assertEquals(0.0, actual9, 0);
		assertEquals(0.0, actual10, 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentWindowedStats#getLastRecord(java.util.HashMap)}.
	 */
	public void testGetLastRecord() {
		HashMap<Integer, Long> records1 = new HashMap<>();
		records1.put(1, 100L);
		records1.put(2, 120L);
		records1.put(3, 135L);
		records1.put(4, 110L);
		records1.put(5, 95L);
		records1.put(6, 78L);
		
		Double actual1 = ComponentWindowedStats.getLastRecord(records1);
		
		HashMap<Integer, Double> records2 = new HashMap<>();
		records2.put(1, 100.0);
		records2.put(2, 120.0);
		records2.put(3, 135.0);
		records2.put(4, 110.0);
		records2.put(5, 95.0);
		records2.put(6, 78.0);
		
		Double actual2 = ComponentWindowedStats.getLastRecord(records2);
		
		HashMap<Integer, Long> records3 = new HashMap<>();
		
		Double actual3 = ComponentWindowedStats.getOldestRecord(records3);
		
		assertEquals(78.0, actual1, 0);
		assertEquals(78.0, actual2, 0);
		assertEquals(0.0, actual3);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentWindowedStats#getOldestRecord(java.util.HashMap)}.
	 */
	public void testGetOldestRecord() {
		HashMap<Integer, Long> records1 = new HashMap<>();
		records1.put(1, 100L);
		records1.put(2, 120L);
		records1.put(3, 135L);
		records1.put(4, 110L);
		records1.put(5, 95L);
		records1.put(6, 78L);
		
		Double actual1 = ComponentWindowedStats.getOldestRecord(records1);
		
		HashMap<Integer, Double> records2 = new HashMap<>();
		records2.put(1, 100.0);
		records2.put(2, 120.0);
		records2.put(3, 135.0);
		records2.put(4, 110.0);
		records2.put(5, 95.0);
		records2.put(6, 78.0);
		
		Double actual2 = ComponentWindowedStats.getOldestRecord(records2);
		
		HashMap<Integer, Long> records3 = new HashMap<>();
		
		Double actual3 = ComponentWindowedStats.getOldestRecord(records3);
				
		assertEquals(100.0, actual1, 0);
		assertEquals(100.0, actual2, 0);
		assertEquals(0.0, actual3);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentWindowedStats#getVariations(java.util.HashMap)}.
	 */
	public void testGetVariations() {
		HashMap<Integer, Long> records1 = new HashMap<>();
		records1.put(1, 100L);
		records1.put(2, 120L);
		records1.put(3, 135L);
		records1.put(4, 110L);
		records1.put(5, 95L);
		records1.put(6, 78L);
		
		ArrayList<Double> actual1 = ComponentWindowedStats.getVariations(records1);
		
		HashMap<Integer, Double> records2 = new HashMap<>();
		records2.put(1, 100.0);
		records2.put(2, 120.0);
		records2.put(3, 135.0);
		records2.put(4, 110.0);
		records2.put(5, 95.0);
		records2.put(6, 78.0);
		
		ArrayList<Double> actual2 = ComponentWindowedStats.getVariations(records2);
		
		ArrayList<Double> expected = new ArrayList<>();
		expected.add(-17.0);
		expected.add(-15.0);
		expected.add(-25.0);
		expected.add(15.0);
		expected.add(20.0);
		
		assertEquals(expected, actual1);
		assertEquals(expected, actual2);
	}

}
