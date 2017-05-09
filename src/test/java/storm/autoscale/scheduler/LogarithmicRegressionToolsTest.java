/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;
import storm.autoscale.scheduler.regression.LogarithmicRegressionTools;

/**
 * @author Roland
 *
 */
public class LogarithmicRegressionToolsTest extends TestCase {
	
	private static HashMap<Integer, Long> coordinatesRandom;
	private static HashMap<Integer, Long> coordinatesLogarithmic;

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Before
	protected void setUp() throws Exception {
		super.setUp();
		coordinatesRandom = new HashMap<>();
		coordinatesRandom.put(1, 0L);
		coordinatesRandom.put(2, 100L);
		coordinatesRandom.put(3, 120L);
		coordinatesRandom.put(4, 40L);
		coordinatesRandom.put(5, 75L);
		coordinatesRandom.put(6, 30L);
		coordinatesRandom.put(7, 90L);
		coordinatesRandom.put(8, 10L);
		
		coordinatesLogarithmic = new HashMap<>();
		coordinatesLogarithmic.put(1, 2L);
		coordinatesLogarithmic.put(2, 40L);
		coordinatesLogarithmic.put(3, 80L);
		coordinatesLogarithmic.put(4, 110L);
		coordinatesLogarithmic.put(5, 104L);
		coordinatesLogarithmic.put(6, 112L);
		coordinatesLogarithmic.put(7, 103L);
		coordinatesLogarithmic.put(8, 116L);
	}

	@Test
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#linearizedCoordinates(java.util.HashMap)}.
	 */
	public void testLinearizedCoordinates() {
		HashMap<Double, Double> expectedRandom = new HashMap<>();
		expectedRandom.put(Math.log(1.0), 0.0);
		expectedRandom.put(Math.log(2.0), 100.0);
		expectedRandom.put(Math.log(3.0), 120.0);
		expectedRandom.put(Math.log(4.0), 40.0);
		expectedRandom.put(Math.log(5.0), 75.0);
		expectedRandom.put(Math.log(6.0), 30.0);
		expectedRandom.put(Math.log(7.0), 90.0);
		expectedRandom.put(Math.log(8.0), 10.0);
		
		HashMap<Double, Double> actualRandom = LogarithmicRegressionTools.linearizeCoordinates(coordinatesRandom);
		
		for(Double x : expectedRandom.keySet()){
			assertEquals(expectedRandom.get(x), actualRandom.get(x), 0.001);
		}
		
		HashMap<Double, Double> expectedLogarithmic = new HashMap<>();
		expectedRandom.put(Math.log(1.0), 2.0);
		expectedRandom.put(Math.log(2.0), 40.0);
		expectedRandom.put(Math.log(3.0), 80.0);
		expectedRandom.put(Math.log(4.0), 110.0);
		expectedRandom.put(Math.log(5.0), 104.0);
		expectedRandom.put(Math.log(6.0), 112.0);
		expectedRandom.put(Math.log(7.0), 103.0);
		expectedRandom.put(Math.log(8.0), 116.0);
		
		HashMap<Double, Double> actualLogarithmic = LogarithmicRegressionTools.linearizeCoordinates(coordinatesLogarithmic);
		
		for(Double x : expectedLogarithmic.keySet()){
			assertEquals(expectedLogarithmic.get(x), actualLogarithmic.get(x), 0.001);
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#regressionCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionCoeff() {
		assertEquals(3.059, LogarithmicRegressionTools.regressionCoeff(coordinatesRandom), 0.001);
		assertEquals(55.914, LogarithmicRegressionTools.regressionCoeff(coordinatesLogarithmic), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#regressionOffset(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionOffset() {
		assertEquals(54.07, LogarithmicRegressionTools.regressionOffset(coordinatesRandom), 0.001);
		assertEquals(9.256, LogarithmicRegressionTools.regressionOffset(coordinatesLogarithmic), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#determinationCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testDeterminationCoeff() {
		assertEquals(0.002, LogarithmicRegressionTools.determinationCoeff(coordinatesRandom), 0.001);
		assertEquals(0.908, LogarithmicRegressionTools.determinationCoeff(coordinatesLogarithmic), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#estimateYCoordinate(java.lang.Number, java.util.HashMap)}.
	 */
	@Test
	public void testEstimateYCoordinate() {
		assertEquals(61, LogarithmicRegressionTools.estimateYCoordinate(12, coordinatesRandom), 1.0);
		assertEquals(148, LogarithmicRegressionTools.estimateYCoordinate(12, coordinatesLogarithmic), 1.0);
	}

}
