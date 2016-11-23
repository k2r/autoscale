/**
 * 
 */
package storm.autoscale.scheduler.connector.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Roland
 *
 */
public interface IJDBCConnector {
	
	public Connection getConnection();
	
	/**
	 * 
	 * @return a Statement object to query tables of the database
	 * @throws SQLException
	 */
	public Statement getNewStatement() throws SQLException ;
	
	/**
	 * 
	 * @param query an update query (INSERT/DELETE/UPDATE) on the specified table 
	 * @return 1 if the query returns a count, 0 otherwise
	 * @throws SQLException
	 */
	public int executeUpdate(String query) throws SQLException;
	
	/**
	 * 
	 * @param query a query (SELECT) on specified tables
	 * @return a ResultSet corresponding to selected rows and columns on specified tables
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String query) throws SQLException;

}
