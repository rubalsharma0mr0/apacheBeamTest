import org.apache.beam.sdk.schemas.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static addColumnBeam.buildSchemaBeam.buildSchema;

public class test {

    public static void main(String[] args) throws IOException {

        String schemaDefinition = new String(Files.readAllBytes(Paths.get("/home/rubal/IdeaProjects/apacheBeam/src/main/java/addColumnBeam/schemaJSON.json")), StandardCharsets.UTF_8);

        Schema newBuildSchema = buildSchema(schemaDefinition);
        List<String> fieldNames = newBuildSchema.getFieldNames();

        StringBuilder incomingFields = new StringBuilder("UPDATE ");
        for (String names : fieldNames) {
            Schema.FieldType type = newBuildSchema.getField(names).getType();
            incomingFields.append("[").append(type).append("]");
        }
        System.out.println(incomingFields);

        JSONArray incomingSchema = new JSONArray(schemaDefinition);
        JSONObject jsonObject;

        StringBuilder selectQuery = new StringBuilder("SELECT ");
        for (int i = 0; i < incomingSchema.length(); i++) {
            jsonObject = incomingSchema.getJSONObject(i);
            String name = jsonObject.getString("name");
            selectQuery.append("[").append(name).append("]").append(",");
        }
        selectQuery.deleteCharAt(selectQuery.lastIndexOf(","));
        System.out.println(selectQuery);

        StringBuilder insertQuery = new StringBuilder("INSERT INTO Persons VALUES [");
        for (int i = 0; i < incomingSchema.length(); i++) {
            jsonObject = incomingSchema.getJSONObject(i);
            String type = jsonObject.getString("type");
            if (type.equals("int32") | type.equals("int") | type.equals("int64")) {
                insertQuery.append(type).append(",");
            } else {
                insertQuery.append("'").append(type).append("'").append(",");
            }
        }
        insertQuery.append("]");
        insertQuery.deleteCharAt(insertQuery.lastIndexOf(","));
        System.out.println(insertQuery);
    }
}
