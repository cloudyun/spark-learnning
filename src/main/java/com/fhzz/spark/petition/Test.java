package com.fhzz.spark.petition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Test {
	
	public static void main(String[] args) throws SQLException {
		
		Connection con = ConnectionPool.getConnection();
		String sql = "insert into face (name, age) values (?, ?)";
		PreparedStatement ps = con.prepareStatement(sql);
		List<FaceData> collect = new ArrayList<FaceData>();
		
		collect.add(new FaceData("zhangsan", 23));
		collect.add(new FaceData("lisi", 32));
		for (FaceData data : collect) {
			int x = 1;
			ps.setString(x++, data.getName());
			ps.setInt(x++, data.getAge());
			ps.addBatch();
		}
		ps.executeBatch();
		ps.close();
		con.close();
		
	}
}
