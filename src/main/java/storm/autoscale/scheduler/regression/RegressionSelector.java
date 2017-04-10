/**
 * 
 */
package storm.autoscale.scheduler.regression;

import java.util.ArrayList;
import java.util.HashMap;

import storm.autoscale.scheduler.util.UtilFunctions;

/**
 * @author Roland
 * @param <T>
 * @param <U>
 *
 */
public class RegressionSelector<T extends Number, U extends Number> {

	private HashMap<T, U> coordinates;
	private String modelType;
	private Double correlation;
	private Double regressionCoeff;
	private Double regressionOffset;
	
	public static final String LINEAR = "linear";
	//public static final String EXP = "exponential";
	//public static final String POW = "power";
	//public static final String LOG = "logarithmic";
	
	public RegressionSelector(HashMap<T, U> coordinates){
		this.coordinates = coordinates;
		
		HashMap<String, Double> correlations = new HashMap<>();
		Double linearCorr = LinearRegressionTools.determinationCoeff(coordinates);
		//Double expCorr = ExponentialRegressionTools.determinationCoeff(coordinates);
		//Double powCorr = PowerRegressionTools.determinationCoeff(coordinates);
		//Double logCorr = LogarithmicRegressionTools.determinationCoeff(coordinates);
		
		correlations.put(LINEAR, linearCorr);
		//correlations.put(EXP, expCorr);
		//correlations.put(POW, powCorr);
		//correlations.put(LOG, logCorr);
		
		this.modelType = LINEAR;
		String maxCategory = UtilFunctions.getMaxCategory(correlations);
		if(!this.isConstantFunction() && maxCategory != null){
			this.modelType = maxCategory;
		}
		
		if(this.modelType.equalsIgnoreCase(LINEAR)){
			this.regressionCoeff = LinearRegressionTools.regressionCoeff(coordinates);
			this.regressionOffset = LinearRegressionTools.regressionOffset(coordinates);
			this.correlation = linearCorr;
		}
		/*if(this.modelType.equalsIgnoreCase(EXP)){
			this.regressionCoeff = ExponentialRegressionTools.regressionCoeff(coordinates);
			this.regressionOffset = ExponentialRegressionTools.regressionOffset(coordinates);
			this.correlation = expCorr;
		}
		if(this.modelType.equalsIgnoreCase(POW)){
			this.regressionCoeff = PowerRegressionTools.regressionCoeff(coordinates);
			this.regressionOffset = PowerRegressionTools.regressionOffset(coordinates);
			this.correlation = powCorr;
		}
		if(this.modelType.equalsIgnoreCase(LOG)){
			this.regressionCoeff = LogarithmicRegressionTools.regressionCoeff(coordinates);
			this.regressionOffset = LogarithmicRegressionTools.regressionOffset(coordinates);
			this.correlation = logCorr;
		}*/
	}
	
	/**
	 * @return the correlation
	 */
	public Double getCorrelation() {
		return correlation;
	}

	public Double getRegressionCoeff(){
		return this.regressionCoeff;
	}
	
	public Double getRegressionOffset(){
		return this.regressionOffset;
	}
	
	public boolean isConstantFunction(){
		boolean isConstant = true;
		ArrayList<U> yCoordinates = new ArrayList<>();
		for(T x : this.coordinates.keySet()){
			yCoordinates.add(this.coordinates.get(x));
		}
		Double value = yCoordinates.get(0).doubleValue();
		int n = yCoordinates.size();
		for(int i = 1; i < n; i++){
			Double yCoordinate = yCoordinates.get(i).doubleValue();
			if(Math.abs(value - yCoordinate) > 0.001){
				isConstant = false;
				break;
			}
		}
		return isConstant;
	}
	
	public Double estimateYCoordinate(T xCoordinate){
		Double result = 0.0;
		
		if(this.modelType.equalsIgnoreCase(LINEAR)){
			result = LinearRegressionTools.estimateYCoordinate(xCoordinate, this.coordinates);
		}
//		if(this.modelType.equalsIgnoreCase(EXP)){
//			result = ExponentialRegressionTools.estimateYCoordinate(xCoordinate, this.coordinates);
//		}
//		if(this.modelType.equalsIgnoreCase(POW)){
//			result = PowerRegressionTools.estimateYCoordinate(xCoordinate, this.coordinates);
//		}
//		if(this.modelType.equalsIgnoreCase(LOG)){
//			result = LogarithmicRegressionTools.estimateYCoordinate(xCoordinate, this.coordinates);
//		}
		
		return result;
	}
	
	@Override
	public String toString(){
		String result = "Regression model : " + this.modelType + ", correlation r : " + this.correlation + ", y = ";
		if(this.modelType.equalsIgnoreCase(LINEAR)){
			result += this.getRegressionCoeff() + "*x + " + this.getRegressionOffset();
		}
//		if(this.modelType.equalsIgnoreCase(EXP)){
//			result += this.getRegressionCoeff() + "*e^(x*" + this.getRegressionOffset() + ")";
//		}
//		if(this.modelType.equalsIgnoreCase(POW)){
//			result += this.getRegressionCoeff() + "*x^" + this.getRegressionOffset();
//		}
//		if(this.modelType.equalsIgnoreCase(LOG)){
//			result += "(" + this.getRegressionCoeff() + "*x) / (" + this.getRegressionOffset() + " + x)";
//		}
		return result;
	}
	
	public String coordinatesToString(){
		String result = "Coordinates: \n";
		for(T x : this.coordinates.keySet()){
			U y = this.coordinates.get(x);
			result += "[x:" + x + ";y:" + y + "]\n";
		}
		return result;
	}
}