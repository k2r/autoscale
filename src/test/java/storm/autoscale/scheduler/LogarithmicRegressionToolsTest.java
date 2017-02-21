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
		expectedRandom.put(1.0/2.0, 0.01);
		expectedRandom.put(1.0/3.0, 0.008);
		expectedRandom.put(1.0/4.0, 0.025);
		expectedRandom.put(1.0/5.0, 0.013);
		expectedRandom.put(1.0/6.0, 0.033);
		expectedRandom.put(1.0/7.0, 0.011);
		expectedRandom.put(1.0/8.0, 0.1);
		
		HashMap<Double, Double> actualRandom = LogarithmicRegressionTools.linearizeCoordinates(coordinatesRandom);
		
		for(Double x : expectedRandom.keySet()){
			assertEquals(expectedRandom.get(x), actualRandom.get(x), 0.001);
		}
		
		HashMap<Double, Double> expectedLogarithmic = new HashMap<>();
		expectedLogarithmic.put(1.0/2.0, 0.025);
		expectedLogarithmic.put(1.0/3.0, 0.0125);
		expectedLogarithmic.put(1.0/4.0, 0.009);
		expectedLogarithmic.put(1.0/5.0, 0.009);
		expectedLogarithmic.put(1.0/6.0, 0.009);
		expectedLogarithmic.put(1.0/7.0, 0.01);
		expectedLogarithmic.put(1.0/8.0, 0.009);
		
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
		assertEquals(Double.NaN, LogarithmicRegressionTools.regressionCoeff(coordinatesRandom), 0.001);
		assertEquals(0.541, LogarithmicRegressionTools.regressionCoeff(coordinatesLogarithmic), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#regressionOffset(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionOffset() {
		assertEquals(Double.NaN, LogarithmicRegressionTools.regressionOffset(coordinatesRandom), 0.001);
		assertEquals(-0.111, LogarithmicRegressionTools.regressionOffset(coordinatesLogarithmic), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#determinationCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testDeterminationCoeff() {
		assertEquals(Double.NaN, LogarithmicRegressionTools.determinationCoeff(coordinatesRandom), 0.001);
		assertEquals(0.847, LogarithmicRegressionTools.determinationCoeff(coordinatesLogarithmic), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LogarithmicRegressionTools#estimateYCoordinate(java.lang.Number, java.util.HashMap)}.
	 */
	@Test
	public void testEstimateYCoordinate() {
		assertEquals(Double.NaN, LogarithmicRegressionTools.estimateYCoordinate(12, coordinatesRandom), 1.0);
		assertEquals(0.04, LogarithmicRegressionTools.estimateYCoordinate(12, coordinatesLogarithmic), 1.0);
	}

}
