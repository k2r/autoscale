/**
 * 
 */
package storm.autoscale.scheduler.config;

/**
 * @author Roland
 *
 */
public enum ParameterNames {
	
	PARAM("parameters"),
	NIMBHOST("nimbus_host"),
	NIMBPORT("nimbus_port"),
	MONITFREQ("monitoring_frequency"),
	WINSIZE("window_size"),
	STABIL("stability_threshold"),
	ALPHA("alpha"),
	HIGHACT("high_activity_threshold"),
	GRACECOEFF("grace_coeff"),
	SLOPE("slope_threshold"),
	DBHOST("db_host"),
	DBNAME("db_name"),
	DBUSER("db_user"),
	DBPWD("db_password");
	
	
	private String name;
	
	private ParameterNames(String name){
		this.name = name;
	}

	public String toString(){
		return this.name;
	}
}
