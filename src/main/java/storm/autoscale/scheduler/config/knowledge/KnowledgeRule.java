/**
 * 
 */
package storm.autoscale.scheduler.config.knowledge;

/**
 * @author Roland
 *
 */
public class KnowledgeRule {

	private String operator;
	private Double lowerBound;
	private Double upperBound;
	private Integer degree;
	private Double reward;
	
	public KnowledgeRule(String op, Double lb, Double ub, Integer deg, Double rwd) {
		this.setOperator(op);
		this.lowerBound = lb;
		this.upperBound = ub;
		this.degree = deg;
		this.reward = rwd;
	}

	/**
	 * @return the operator
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * @param operator the operator to set
	 */
	public void setOperator(String operator) {
		this.operator = operator;
	}

	/**
	 * @return the lowerBound
	 */
	public Double getLowerBound() {
		return lowerBound;
	}

	/**
	 * @param lowerBound the lowerBound to set
	 */
	public void setLowerBound(Double lowerBound) {
		this.lowerBound = lowerBound;
	}

	/**
	 * @return the upperBound
	 */
	public Double getUpperBound() {
		return upperBound;
	}

	/**
	 * @param upperBound the upperBound to set
	 */
	public void setUpperBound(Double upperBound) {
		this.upperBound = upperBound;
	}

	/**
	 * @return the degree
	 */
	public Integer getDegree() {
		return degree;
	}

	/**
	 * @param degree the degree to set
	 */
	public void setDegree(Integer degree) {
		this.degree = degree;
	}

	/**
	 * @return the reward
	 */
	public Double getReward() {
		return reward;
	}

	/**
	 * @param reward the reward to set
	 */
	public void setReward(Double reward) {
		this.reward = reward;
	}
	
	public Boolean isApplicable(Double value){
		return (this.lowerBound < value && this.upperBound >= value);
	}
	
	@Override
	public boolean equals(Object o){
		KnowledgeRule rule = (KnowledgeRule) o;
		return (this.operator == rule.getOperator() && this.lowerBound == rule.getLowerBound() && this.upperBound == rule.getUpperBound() && this.degree == rule.getDegree() && this.reward == rule.getReward());
	}
}