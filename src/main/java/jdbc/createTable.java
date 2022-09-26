package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;


public class createTable {
    static String username = "root";
    static String password = "password";
    static String url = "jdbc:mysql://127.0.0.1:3306/check123?useSSL=false";

    public static void main(String[] args) {

        Connection conn = null;
        Statement stmt = null;

        try {
            Random rand = new Random();
            int n = rand.nextInt();  // generate random number
            n = Math.abs(n);         // take absolute value
            String tblName = "tmp_tbl_" + n;

            String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS check123." + tblName + " (" + "UID INT NOT NULL," + "NAME VARCHAR(45) NOT NULL," +
                    "DOB DATE NOT NULL," + "EMAIL VARCHAR(45) NOT NULL," + "PRIMARY KEY (UID))";

            conn = DriverManager.getConnection(url, username, password);
            stmt = conn.createStatement();

            stmt.executeUpdate(CREATE_TABLE_SQL);

            System.out.println("Table created");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close connection
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}