package mamba.query;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by dongbin on 2016/10/10.
 */
public interface ConnectionProvider {
  public Connection getConnection() throws SQLException;
}
