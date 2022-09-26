package addColumnBeam;

import org.apache.beam.sdk.schemas.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

public class buildSchemaBeam {
    public static Schema buildSchema(String schemaDefinition) {

        JSONArray incomingSchema = new JSONArray(schemaDefinition);
        Schema.Builder builder = Schema.builder();

        JSONObject jsonObject;
        for (int i = 0; i < incomingSchema.length(); i++) {
            jsonObject = incomingSchema.getJSONObject(i);

            String name = jsonObject.getString("name");
            String type = jsonObject.getString("type");

            switch (type.toLowerCase()) {
                case "int":
                case "number":
                case "int32":
                    Schema.Field int32Field = Schema.Field.nullable(name, Schema.FieldType.INT32);
                    builder.addField(int32Field);
                    break;
                case "int64":
                    Schema.Field int64Field = Schema.Field.nullable(name, Schema.FieldType.INT64);
                    builder.addField(int64Field);
                    break;
                case "varchar":
                case "string":
                case "text":
                    Schema.Field stringField = Schema.Field.nullable(name, Schema.FieldType.STRING);
                    builder.addField(stringField);
            }
        }
        return builder.build();
    }
}
