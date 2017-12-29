package com.fhzz.spark.petition;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import scala.Option;
import scala.Some;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class ConnectionPool implements Serializable {
	
	private static final long serialVersionUID = 5330034078412362930L;
	
	private static Option<BoneCP> apply = null;
	
	private static void init() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl("jdbc:mysql://master1:3306/test");
			config.setUsername("root");
			config.setPassword("123456");
			config.setLazyInit(true);

			config.setMinConnectionsPerPartition(3);
			config.setMaxConnectionsPerPartition(5);
			config.setPartitionCount(5);
			config.setCloseConnectionWatch(true);
			config.setLogStatementsEnabled(false);
			
			apply = Some.apply(new BoneCP(config));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection() {
		if (apply == null) {
			init();
		}
		try {
			return apply.get().getConnection();
		} catch (SQLException e) {
			return null;
		}
	}
	
	public static void colse(Connection con) {
		try {
			if (con != null && !con.isClosed()) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
