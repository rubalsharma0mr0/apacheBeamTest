package jdbc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class printToCsv {
    static List<String> resultArray = new ArrayList<>();
    static String username = "root";
    static String password = "password";
    static String url = "jdbc:mysql://127.0.0.1:3306/check123?useSSL=false";

    public printToCsv(List<String> resultArray, String file_name) throws IOException {
        Path path = Paths.get("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/JDBC");
        File csvOutput = new File(String.valueOf(path), file_name);
        FileWriter fileWriter = new FileWriter(csvOutput, false);

        for (String mapping : resultArray) {
            fileWriter.write(mapping + "\n");

        }
        fileWriter.close();

    }

    public static void main(String[] args) throws SQLException, IOException {

        Connection connection = DriverManager.getConnection(url, username, password);
        String sqlQuery = "SELECT * FROM check123.Persons";

        fetchDataFromDB(sqlQuery, connection);
        new printToCsv(resultArray, "printToCsv.csv");

    }

    private static void fetchDataFromDB(String sqlQuery, Connection connection) throws SQLException {

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlQuery);
        int numColumn = resultSet.getMetaData().getColumnCount();

        while (resultSet.next()) {
            StringBuilder stringBuilder = new StringBuilder();

            for (int i = 1; i <= numColumn; i++) {
                stringBuilder.append(resultSet.getString(i));
            }
            resultArray.add(stringBuilder.toString());
        }
    }
}