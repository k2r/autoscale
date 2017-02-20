/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;
import storm.autoscale.scheduler.regression.ExponentialRegressionTools;
/**
 * @author Roland
 *
 */
public class ExponentialRegressionToolsTest extends TestCase{

	private static HashMap<Integer, Long> coordinatesRandom;
	private static HashMap<Integer, Long> coordinatesExponential;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
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
		
		coordinatesExponential = new HashMap<>();
		coordinatesExponential.put(1, 10L);
		coordinatesExponential.put(2, 11L);
		coordinatesExponential.put(3, 8L);
		coordinatesExponential.put(4, 13L);
		coordinatesExponential.put(5, 19L);
		coordinatesExponential.put(6, 40L);
		coordinatesExponential.put(7, 70L);
		coordinatesExponential.put(8, 105L);
	}

	@Test
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.ExponentialRegressionTools#linearizedCoordinates(java.util.HashMap)}.
	 */
	public void testLinearizedCoordinates() {
		HashMap<Double, Double> expectedRandom = new HashMap<>();
		expectedRandom.put(1.0, 0.0);
		expectedRandom.put(2.0, 4.605);
		expectedRandom.put(3.0, 4.787);
		expectedRandom.put(4.0, 3.689);
		expectedRandom.put(5.0, 4.317);
		expectedRandom.put(6.0, 3.401);
		expectedRandom.put(7.0, 4.5);
		expectedRandom.put(8.0, 2.303);
		
		HashMap<Double, Double> actualRandom = ExponentialRegressionTools.linearizeCoordinates(coordinatesRandom);
		
		for(Double x : expectedRandom.keySet()){
			assertEquals(expectedRandom.get(x), actualRandom.get(x), 0.001);
		}
		
		HashMap<Double, Double> expectedExponential = new HashMap<>();
		expectedExponential.put(1.0, 2.303);
		expectedExponential.put(2.0, 2.398);
		expectedExponential.put(3.0, 2.079);
		expectedExponential.put(4.0, 2.565);
		expectedExponential.put(5.0, 2.944);
		expectedExponential.put(6.0, 3.689);
		expectedExponential.put(7.0, 4.248);
		expectedExponential.put(8.0, 4.654);
		
		HashMap<Double, Double> actualExponential = ExponentialRegressionTools.linearizeCoordinates(coordinatesExponential);
		
		for(Double x : expectedExponential.keySet()){
			assertEquals(expectedExponential.get(x), actualExponential.get(x), 0.001);
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.ExponentialRegressionTools#regressionCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionCoeff() {
		assertEquals(0.143, ExponentialRegressionTools.regressionCoeff(coordinatesRandom), 0.001);
		assertEquals(0.368, ExponentialRegressionTools.regressionCoeff(coordinatesExponential), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.ExponentialRegressionTools#regressionOffset(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionOffset() {
		assertEquals(2.804, ExponentialRegressionTools.regressionOffset(coordinatesRandom), 0.001);
		assertEquals(1.454, ExponentialRegressionTools.regressionOffset(coordinatesExponential), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.ExponentialRegressionTools#correlationCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testCorrelationCoeff() {
		assertEquals(0.217, ExponentialRegressionTools.correlationCoeff(coordinatesRandom), 0.001);
		assertEquals(0.931, ExponentialRegressionTools.correlationCoeff(coordinatesExponential), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.ExponentialRegressionTools#estimateYCoordinate(java.lang.Number, java.util.HashMap)}.
	 */
	@Test
	public void testEstimateYCoordinate() {
		assertEquals(59060433227548.0, ExponentialRegressionTools.estimateYCoordinate(12, coordinatesRandom), 1.0);
		assertEquals(13854892.0, ExponentialRegressionTools.estimateYCoordinate(12, coordinatesExponential), 1.0);
	}

}
