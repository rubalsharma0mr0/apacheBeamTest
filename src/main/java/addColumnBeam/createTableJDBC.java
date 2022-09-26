package addColumnBeam;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

public class createTableJDBC {
    static Connection connection;
    static PreparedStatement preparedStatement;
    static Statement statement;

    public static String createTable(String tableName, String url, String username, String password) {
        String finalTableName = null;
        try {
            String sqlQuery = "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA='check123'";
            connection = DriverManager.getConnection(url, username, password);
            preparedStatement = connection.prepareStatement(sqlQuery);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            ArrayList<String> objects = new ArrayList<>();
            while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String cn = resultSet.getString(i);
                    objects.add(String.valueOf(cn));
                }
            }
            System.out.println("Tables in DataBase are" + objects);

            if (objects.contains(tableName)) {

                Random rand = new Random();
                int n = rand.nextInt();  // generate random number
                n = Math.abs(n);         // take absolute value
                String tblName = "Temporary_Tables_" + n;

                String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS check123." + tblName + "(PersonID int NOT NULL PRIMARY KEY, LastName varchar(255) null,FirstName varchar(255) null,State varchar(255) null,City varchar(255) null,targetColumn varchar(255) null)";

                Statement statement = connection.createStatement();
                statement.executeUpdate(CREATE_TABLE_SQL);
                System.out.println("Final Table created with Name: " + tblName);
                finalTableName = tblName;

            } else {

                String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS check123." + tableName + "(PersonID int NOT NULL PRIMARY KEY, LastName varchar(255) null,FirstName varchar(255) null,State varchar(255) null,City varchar(255) null,targetColumn varchar(255) null)";

                statement = connection.createStatement();
                statement.executeUpdate(CREATE_TABLE_SQL);
                System.out.println("Final Table created with Name: " + tableName);
                finalTableName = tableName;
            }

        } catch (SQLException e) {
            e.printStackTrace();

        } /*finally {
            try {
                // Close connection
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }*/
        return finalTableName;
    }
}
