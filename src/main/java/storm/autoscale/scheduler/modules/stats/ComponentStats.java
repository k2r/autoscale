/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

/**
 * @author Roland
 *
 */
public class ComponentStats {

	private String id;
	private Double nbInputs;
	private Double nbExecuted;
	private Double nbOutputs;
	private Double avgLatency;
	private Double selectivity;
	
	/**
	 * 
	 */
	public ComponentStats(String id, Double nbInputs, Double nbExecuted, Double nbOutputs, Double avgLatency) {
		this.id = id;
		this.nbInputs = nbInputs;
		this.nbExecuted = nbExecuted;
		this.nbOutputs = nbOutputs;
		this.avgLatency = avgLatency;
		this.selectivity = nbOutputs / nbExecuted;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the nbInputs
	 */
	public Double getNbInputs() {
		return nbInputs;
	}

	/**
	 * @param nbInputs the nbInputs to set
	 */
	public void setNbInputs(Double nbInputs) {
		this.nbInputs = nbInputs;
	}

	/**
	 * @return the nbExecuted
	 */
	public Double getNbExecuted() {
		return nbExecuted;
	}

	/**
	 * @param nbExecuted the nbExecuted to set
	 */
	public void setNbExecuted(Double nbExecuted) {
		this.nbExecuted = nbExecuted;
	}

	/**
	 * @return the nbOutputs
	 */
	public Double getNbOutputs() {
		return nbOutputs;
	}

	/**
	 * @param nbOutputs the nbOutputs to set
	 */
	public void setNbOutputs(Double nbOutputs) {
		this.nbOutputs = nbOutputs;
	}

	/**
	 * @return the avgLatency
	 */
	public Double getAvgLatency() {
		return avgLatency;
	}

	/**
	 * @param avgLatency the avgLatency to set
	 */
	public void setAvgLatency(Double avgLatency) {
		this.avgLatency = avgLatency;
	}

	/**
	 * @return the selectivity
	 */
	public Double getSelectivity() {
		return selectivity;
	}

	/**
	 * @param selectivity the selectivity to set
	 */
	public void setSelectivity(Double selectivity) {
		this.selectivity = selectivity;
	}

	
}
