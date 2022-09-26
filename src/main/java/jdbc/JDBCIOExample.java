package jdbc;

/*JDBC or Java Database Connectivity is a Java API to connect and execute the query with the database.
It is a specification from Sun microsystems that provides a standard abstraction(API or Protocol) for java applications to communicate
with various databases. It provides the language with java database connectivity standards. It is used to write programs required to access databases.
JDBC, along with the database driver, can access databases and spreadsheets.
The enterprise data stored in a relational database(RDB) can be accessed with the help of JDBC APIs.*/

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

import java.sql.*;

public class JDBCIOExample {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

       /* PCollection<String> pCollectionInput = pipeline.apply(JdbcIO.<String>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/Sample?useSSL=false")
                        .withUsername("root")
                        .withPassword("password"))
                .withQuery("SELECT * FROM Sample.agents")
                .withRowMapper(new JdbcIO.RowMapper<String>() {

                    public String mapRow(ResultSet resultSet) throws Exception {

//                        System.out.println(resultSet.toString());
                        return resultSet.getString(1) +
                                "," + resultSet.getString(2) +
                                "," + resultSet.getString(3) +
                                "," + resultSet.getString(4) +
                                "," + resultSet.getString(5);
                    }
                })
        );

        pCollectionInput.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/JDBC/JDBCIOExample1.csv")
                .withHeader("AGENT_CODE, AGENT_NAME, WORKING_AREA, COMMISSION, PHONE_NO")
                .withNumShards(1).withSuffix(".csv"));*/


        String connString = "jdbc:mysql://127.0.0.1:3306/HR?useSSL=false";
        String password = "password";
        String user_name = "root";
        try {
            Connection connection = DriverManager.getConnection(connString, user_name, password);
            Statement stmt = connection.createStatement();
//            stmt.execute("INSERT INTO jobs VALUE (22, 'Influencer marketing', 1450.00,  50000.00)");

            PCollection<String> pCollectionInput1 = pipeline.apply(JdbcIO.<String>read().
                    withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/HR?useSSL=false")
                            .withUsername("root")
                            .withPassword("password"))
                    .withQuery("SELECT * FROM jobs")
                    .withRowMapper(new JdbcIO.RowMapper<String>() {

                        public String mapRow(ResultSet resultSet) throws Exception {
//                            System.out.println(resultSet);
                            return resultSet.getString(1) +
                                    "," + resultSet.getString(2) +
                                    "," + resultSet.getString(3) +
                                    "," + resultSet.getString(4);
                        }
                    }));
            pCollectionInput1.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/JDBC/JDBCIOExample2.csv")
                    .withHeader("job_id, job_title, min_salary, max_salary")
                    .withNumShards(1).withSuffix(".csv"));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        pipeline.run();
    }
}































