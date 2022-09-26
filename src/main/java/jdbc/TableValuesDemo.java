package jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;


public class TableValuesDemo {
    public static void main(String[] args) {
        Connection con;
        Statement statement;
        try {

            HashMap<Object, Object> hm = new HashMap<>();

            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/HR", "root", "password");

            statement = con.createStatement();
            String sql = "SELECT * FROM jobs";

            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                hm.put(resultSet.getString("job_id"), resultSet.getString("job_title"));
            }
            System.out.println(hm);

            String str = String.valueOf(hm);

            byte[] arr = str.getBytes();

            Path path = Paths.get("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/JDBC/JDBCIOExample3.csv");

            Files.write(path, arr);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}