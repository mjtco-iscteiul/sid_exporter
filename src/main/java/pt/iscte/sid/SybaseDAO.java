import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SybaseDAO {

	public Connection connect(String[] properties, boolean valid) {
		String arg = "jdbc4";
		if(valid) {
		try {
			Connection con = DriverManager.getConnection("jdbc:sqlanywhere:uid="+properties[6]+";pwd="+properties[1]+";eng="+properties[2]+";host="+properties[11]+":"+properties[4]);
			System.out.println("Using "+arg+" driver");
			con.setTransactionIsolation(sap.jdbc4.sqlanywhere.IConnection.SA_TRANSACTION_SNAPSHOT);//SET OPTION PUBLIC.allow_snapshot_isolation = 'On'; <- SQL ANYWHERE
			con.setAutoCommit(false);
			return con;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}}else {
			try {
				Connection con = DriverManager.getConnection("jdbc:sqlanywhere:uid=G29;pwd=123456;eng=G292DB;host=172.17.15.142:2638");
				System.out.println("Using "+arg+" driver");
				con.setTransactionIsolation(sap.jdbc4.sqlanywhere.IConnection.SA_TRANSACTION_SNAPSHOT);//SET OPTION PUBLIC.allow_snapshot_isolation = 'On'; <- SQL ANYWHERE
				con.setAutoCommit(false);
				return con;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	public void disconnect(Connection con) {
		try {
			con.commit();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int selectRecentDate(Connection con) {
		Statement stmt;
		int time = 0;
		try {
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(DataInicio) FROM DataMigracao WHERE DataFim is not NULL;");

			while(rs.next()) {
				try {
					time = (int) rs.getTimestamp(1).toInstant().getEpochSecond();
				}catch(NullPointerException e) {
					rs.close();
					stmt.close();
				}

			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return time;
	}

	public int selectCurrentDate(Connection con) {
		Statement stmt;
		int time = 0;
		try {
			stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery("SELECT MAX(DataInicio) FROM DataMigracao;");
			while(rs.next()) {
				time = (int) rs.getTimestamp(1).toInstant().getEpochSecond();
			}
			rs.close();
			stmt.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return time;
	}
	
	public void prepareInsert(PreparedStatement ps, String param1, String param2, String param3, String param4) {
		try {
			ps.setString(1, param1);
			ps.setString(2, param2);
			ps.setString(3, param3);
			ps.setString(4, param4);
			ps.addBatch();
		} catch (SQLException e) {
			System.out.println("Invalid insert!");
		}
	}
	
	public PreparedStatement createStatement(Connection con, String param1, String param2, String param3, String param4) throws SQLException {
		PreparedStatement ps = con.prepareStatement("INSERT INTO HumidadeTemperatura (DataMedicao, HoraMedicao, ValorMedicaoTemperatura, ValorMedicaoHumidade) VALUES (?,?,?,?);");
		ps.setString(1, param1);
		ps.setString(2, param2);
		ps.setString(3, param3);
		ps.setString(4, param4);
		ps.addBatch();
		return ps;
	}
	
	public void executeStatement(PreparedStatement ps) throws SQLException, NullPointerException {
		ps.executeBatch();
		ps.close();
	}

	public void inicioMigracao(Connection con) {
		PreparedStatement ps;
		try {
			ps = con.prepareStatement("INSERT INTO DataMigracao (DataInicio, DataFim) VALUES (now(),null);");

			ps.executeUpdate();
			ps.close();
			con.commit();//hmm
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void fimMigracao(Connection con) {
		PreparedStatement ps;
		try {
			ps = con.prepareStatement("UPDATE DataMigracao SET DataFim = now() WHERE DataInicio LIKE (SELECT TOP 1 DataInicio FROM DataMigracao ORDER BY DataInicio DESC);");

			ps.executeUpdate();
			ps.close();
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
