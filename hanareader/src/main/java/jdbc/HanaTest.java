package jdbc;
import java.sql.*;

public class HanaTest {
    public static String connectionString = "jdbc:sap://192.168.33.131:30041";
    public static String user = "ZJDBC01";
    public static String password = "YKbasis123";

    public static void main(String[] argv) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionString, user, password);
        } catch (SQLException e) {
            System.err.println("Connection Failed. User/Passwd Error? Message: " + e.getMessage());
            return;
        }
        if (connection != null) {
            try {
                System.out.println("Connection to HANA successful!");
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery("select * from \"SAPHANADB\".\"COKA\"");
                resultSet.next();
                String hello = resultSet.getString(1);
                System.out.println(hello);
            } catch (SQLException e) {
                System.err.println("Query failed!");
            }
        }
    }
}

