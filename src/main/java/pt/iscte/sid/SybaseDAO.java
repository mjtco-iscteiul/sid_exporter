package pt.iscte.sid;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SybaseDAO {

    public Connection connect() {
        String arg = "jdbc4";
        try {
            Connection con = DriverManager.getConnection("jdbc:sqlanywhere:uid=G29;pwd=123456;eng=G292DB;host=172.17.15.142:2638");
            System.out.println("Using "+arg+" driver");
            con.setTransactionIsolation(sap.jdbc4.sqlanywhere.IConnection.SA_TRANSACTION_SNAPSHOT);//SET OPTION PUBLIC.allow_snapshot_isolation = 'On'; <- SQL ANYWHERE
            return con;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
            ResultSet rs = stmt.executeQuery("SELECT MAX(DataInicio) FROM DataMigracao WHERE DataFim IS NOT NULL;");
            while(rs.next()) {
                try {
                    time = (int) rs.getTimestamp(1).toInstant().getEpochSecond();
                } catch(NullPointerException e) {
                    rs.close();
                    stmt.close();
                }

            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.out.println("First Migration Started.");
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
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

//    public void selectFromInvestigador(Connection con){
//        Statement stmt;
//        try {
//            stmt = con.createStatement();
//
//            ResultSet rs = stmt.executeQuery("SELECT * FROM Investigador;");
//
//            while (rs.next()){
//                int value = rs.getInt(1);
//                String FirstName = rs.getString(2);
//                System.out.println(value+" "+FirstName);
//            }
//
//            rs.close();
//            stmt.close();
//        } catch (SQLException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//
//    public void insertToInvestigador(Connection con, int id, String nome){
//        PreparedStatement ps;
//        try {
//            ps = con.prepareStatement("INSERT INTO Investigador (id, name) VALUES (?,?);");
//
//            ps.setInt(1, id);
//            ps.setString(2, nome);
//
//            ps.executeUpdate();
//            ps.close();
//        } catch (SQLException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//
//    public void insertHumidadeTemperatura(Connection con, String DataMedicao, String HoraMedicao, String ValorMedicaoTemperatura, String ValorMedicaoHumidade) throws SQLException {
//        PreparedStatement ps;
//        ps = con.prepareStatement("INSERT INTO HumidadeTemperatura (DataMedicao, HoraMedicao, ValorMedicaoTemperatura, ValorMedicaoHumidade) VALUES (?,?,?,?);");//addBatch() para varios inserts de uma so vez
//
//        ps.setString(1, DataMedicao);
//        ps.setString(2, HoraMedicao);
//        ps.setString(3, ValorMedicaoTemperatura);
//        ps.setString(4, ValorMedicaoHumidade);
//
//        ps.executeUpdate();
//        ps.close();
//    }

}
