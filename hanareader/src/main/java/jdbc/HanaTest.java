package jdbc;
import cn.ctyun.datax.plugin.reader.hanareader.KeyConstant;

import java.sql.*;

public class HanaTest {
    public static String connectionString = "jdbc:sap://192.168.33.131:30041/SAPHANADB?reconnect";
    public static String user = "ZJDBC01";
    public static String password = "YKbasis123";

    public static void main(String[] argv) {
        Connection connection = null;
        try {
            Class.forName(KeyConstant.DRIVER);
            connection = DriverManager.getConnection(connectionString, user, password);
        } catch (SQLException e) {
            System.err.println("Connection Failed. User/Passwd Error? Message: " + e.getMessage());
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (connection != null) {
            try {
                System.out.println("Connection to HANA successful!");
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT MANDT , OBJNR , GJAHR , KSTAR , HRKFT , MEINH , MGEFL , EIGEN  from \"SAPHANADB\".\"COKA\"");
                resultSet.next();
                String hello = resultSet.getString(1);
                System.out.println(hello);
            } catch (SQLException e) {
                System.err.println("Query failed!");
            }
        }
    }
}