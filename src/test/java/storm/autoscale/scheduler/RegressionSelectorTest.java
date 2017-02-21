/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;
import storm.autoscale.scheduler.regression.RegressionSelector;

/**
 * @author Roland
 *
 */
public class RegressionSelectorTest extends TestCase{

	private static HashMap<Integer, Long> coordinatesRandom;
	private static HashMap<Integer, Long> coordinatesLinear;
	private static HashMap<Integer, Long> coordinatesExponential;
	private static HashMap<Integer, Long> coordinatesPower;
	private static HashMap<Integer, Long> coordinatesLogarithmic;
	private static HashMap<Integer, Long> coordinatesConstant;
	
	private static RegressionSelector<Integer, Long> regressionRandom;
	private static RegressionSelector<Integer, Long> regressionLinear;
	private static RegressionSelector<Integer, Long> regressionExponential;
	private static RegressionSelector<Integer, Long> regressionPower;
	private static RegressionSelector<Integer, Long> regressionLogarithmic;
	private static RegressionSelector<Integer, Long> regressionConstant;
	
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
		
		coordinatesLinear = new HashMap<>();
		coordinatesLinear.put(1, 0L);
		coordinatesLinear.put(2, 15L);
		coordinatesLinear.put(3, 20L);
		coordinatesLinear.put(4, 40L);
		coordinatesLinear.put(5, 56L);
		coordinatesLinear.put(6, 68L);
		coordinatesLinear.put(7, 90L);
		coordinatesLinear.put(8, 110L);
		
		coordinatesExponential = new HashMap<>();
		coordinatesExponential.put(1, 10L);
		coordinatesExponential.put(2, 11L);
		coordinatesExponential.put(3, 8L);
		coordinatesExponential.put(4, 13L);
		coordinatesExponential.put(5, 19L);
		coordinatesExponential.put(6, 40L);
		coordinatesExponential.put(7, 70L);
		coordinatesExponential.put(8, 105L);
		
		coordinatesPower = new HashMap<>();
		coordinatesPower.put(1, 0L);
		coordinatesPower.put(2, 2L);
		coordinatesPower.put(3, 3L);
		coordinatesPower.put(4, 6L);
		coordinatesPower.put(5, 9L);
		coordinatesPower.put(6, 13L);
		coordinatesPower.put(7, 17L);
		coordinatesPower.put(8, 21L);
		
		coordinatesLogarithmic = new HashMap<>();
		coordinatesLogarithmic.put(1, 2L);
		coordinatesLogarithmic.put(2, 40L);
		coordinatesLogarithmic.put(3, 80L);
		coordinatesLogarithmic.put(4, 110L);
		coordinatesLogarithmic.put(5, 104L);
		coordinatesLogarithmic.put(6, 112L);
		coordinatesLogarithmic.put(7, 103L);
		coordinatesLogarithmic.put(8, 116L);
		
		coordinatesConstant = new HashMap<>();
		coordinatesConstant.put(1, 20L);
		coordinatesConstant.put(2, 20L);
		coordinatesConstant.put(3, 20L);
		coordinatesConstant.put(4, 20L);
		coordinatesConstant.put(5, 20L);
		coordinatesConstant.put(6, 20L);
		coordinatesConstant.put(7, 20L);
		coordinatesConstant.put(8, 20L);
		
		regressionRandom = new RegressionSelector<>(coordinatesRandom);
		regressionLinear = new RegressionSelector<>(coordinatesLinear);
		regressionExponential = new RegressionSelector<>(coordinatesExponential);
		regressionPower = new RegressionSelector<>(coordinatesPower);
		regressionLogarithmic = new RegressionSelector<>(coordinatesLogarithmic);
		regressionConstant = new RegressionSelector<>(coordinatesConstant);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.RegressionSelector#isConstantFunction()}.
	 */
	@Test
	public void testIsConstantFunction() {
		assertEquals(false, regressionLinear.isConstantFunction());
		assertEquals(false, regressionExponential.isConstantFunction());
		assertEquals(false, regressionPower.isConstantFunction());
		assertEquals(false, regressionLogarithmic.isConstantFunction());
		assertEquals(true, regressionConstant.isConstantFunction());
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.RegressionSelector#getCorrelationCoeff()}.
	 */
	@Test
	public void testGetCorrelationCoeff() {
		assertEquals(0.578, regressionRandom.getCorrelation(), 0.001);
		assertEquals(0.986, regressionLinear.getCorrelation(), 0.001);
		assertEquals(0.867, regressionExponential.getCorrelation(), 0.001);
		assertEquals(0.97, regressionPower.getCorrelation(), 0.001);	
		assertEquals(0.846, regressionLogarithmic.getCorrelation(), 0.001);
		assertEquals(1.0, regressionConstant.getCorrelation(), 0.001);
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.RegressionSelector#getRegressionCoeff()}.
	 */
	@Test
	public void testGetRegressionCoeff() {
		assertEquals(285.959, regressionRandom.getRegressionCoeff(), 0.001);
		assertEquals(15.536, regressionLinear.getRegressionCoeff(), 0.001);
		assertEquals(0.3681, regressionExponential.getRegressionCoeff(), 0.001);
		assertEquals(3.036, regressionPower.getRegressionCoeff(), 0.001);	
		assertEquals(0.541, regressionLogarithmic.getRegressionCoeff(), 0.001);
		assertEquals(0.0, regressionConstant.getRegressionCoeff(), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.RegressionSelector#getRegressionOffset()}.
	 */
	@Test
	public void testGetRegressionOffset() {
		assertEquals(-468.664, regressionRandom.getRegressionOffset(), 0.001);
		assertEquals(-20.036, regressionLinear.getRegressionOffset(), 0.001);
		assertEquals(1.454, regressionExponential.getRegressionOffset(), 0.001);
		assertEquals(-4.786, regressionPower.getRegressionOffset(), 0.001);		
		assertEquals(-0.111, regressionLogarithmic.getRegressionOffset(), 0.001);
		assertEquals(20.0, regressionConstant.getRegressionOffset(), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.RegressionSelector#estimateYCoordinate(java.lang.Number)}.
	 */
	@Test
	public void testEstimateYCoordinate() {
		assertEquals(0.0, regressionRandom.estimateYCoordinate(12), 0.001);
		assertEquals(166.393, regressionLinear.estimateYCoordinate(12), 0.001);
		assertEquals(13854892.513, regressionExponential.estimateYCoordinate(12), 0.001);
		assertEquals(31.643, regressionPower.estimateYCoordinate(12), 0.001);		
		assertEquals(0.546, regressionLogarithmic.estimateYCoordinate(12), 0.001);
		assertEquals(20.0, regressionConstant.estimateYCoordinate(12), 0.001);
	}

}
