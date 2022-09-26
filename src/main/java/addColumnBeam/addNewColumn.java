package addColumnBeam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;

import static addColumnBeam.buildSchemaBeam.buildSchema;
import static addColumnBeam.createTableJDBC.createTable;


public class addNewColumn {
    static String driver = "com.mysql.cj.jdbc.Driver";
    static String username = "root";
    static String password = "password";
    static String url = "jdbc:mysql://127.0.0.1:3306/check123?useSSL=false";
    static String sqlQuery = "SELECT * FROM check123.Person";
    static String insertTableName = "rubal";

    public static void main(String[] args) throws SQLException, IOException, InterruptedException {

        Pipeline pipeline = Pipeline.create();

        String schemaDefinition = new String(Files.readAllBytes(Paths.get("/home/rubal/IdeaProjects/apacheBeam/src/main/java/addColumnBeam/schemaJSON.json")), StandardCharsets.UTF_8);

        Schema newBuildSchema = buildSchema(schemaDefinition);

        PCollection<Row> pCollectionRead = pipeline.apply(JdbcIO.readRows().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, url).withUsername(username).withPassword(password)).withQuery(sqlQuery)).setRowSchema(newBuildSchema).setCoder(SchemaCoder.of(newBuildSchema));

        String targetColumn = "targetColumn";
        PCollection<Row> addColumn = pCollectionRead.apply(SqlTransform.query("SELECT * , CAST( NULL AS VARCHAR) AS " + targetColumn + " FROM PCOLLECTION"));

//        System.out.println(addColumn.getSchema());

        if (insertTableName != null) {
            String finalTableName = createTable(insertTableName, url, username, password);

            addColumn.apply(JdbcIO.<Row>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, url).withUsername(username).withPassword(password)).withTable(finalTableName).withStatement("INSERT INTO" + finalTableName + "('PersonID', 'LastName','FirstName','Address','City', 'targetColumn') VALUES (?,?,?,?,?,?)"));
        } else {

            addColumn.apply(JdbcIO.<Row>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, url).withUsername(username).withPassword(password)).withTable(insertTableName).withStatement("INSERT INTO " + insertTableName + " ('PersonID', 'LastName','FirstName','Address','City', 'targetColumn') VALUES (?,?,?,?,?,?)"));
        }
        pipeline.run();
    }
}
