/**
 * 
 */
package storm.psrl.scheduler.util;

import java.util.ArrayList;

import backtype.storm.scheduler.ExecutorDetails;

/**
 * @author Roland
 *
 */
public class ExecutorUIdentifier {

	private ExecutorDetails ed;
	private String compId;
	private String topId;
	
	/**
	 * 
	 */
	public ExecutorUIdentifier(ExecutorDetails ed, String compId, String topId) {
		this.ed = ed;
		this.compId = compId;
		this.topId = topId;
	}

	/**
	 * @return the ed
	 */
	public ExecutorDetails getEd() {
		return ed;
	}

	/**
	 * @param ed the ed to set
	 */
	public void setEd(ExecutorDetails ed) {
		this.ed = ed;
	}

	/**
	 * @return the compId
	 */
	public String getCompId() {
		return compId;
	}

	/**
	 * @param compId the compId to set
	 */
	public void setCompId(String compId) {
		this.compId = compId;
	}

	/**
	 * @return the topId
	 */
	public String getTopId() {
		return topId;
	}

	/**
	 * @param topId the topId to set
	 */
	public void setTopId(String topId) {
		this.topId = topId;
	}
	
	public boolean equals(ExecutorUIdentifier eui){
		return (this.getEd().equals(eui.getEd()) && this.getCompId().equalsIgnoreCase(eui.getCompId()) && this.getTopId().equalsIgnoreCase(eui.getTopId()));
	}
	
	public boolean isFork(ExecutorUIdentifier eui){
		return (this.getCompId().equalsIgnoreCase(eui.getCompId()) && this.getTopId().equalsIgnoreCase(eui.getTopId()));
	}
	
	public boolean hasFork(ArrayList<ExecutorUIdentifier> list){
		for(ExecutorUIdentifier eui : list){
			if(this.isFork(eui)){
				return true;
			}
		}
		return false;
	}

}
