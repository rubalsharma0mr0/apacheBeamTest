package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<CustomerEntity> pListApply = pipeline.apply(Create.of(getCustomers()));

        PCollection<String> pString = pListApply.apply(MapElements.into(TypeDescriptors.strings()).via(
                (CustomerEntity customerEntity) -> customerEntity.getName()
        ));

        pString.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.InMemoryExample.csv").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }

    static List<CustomerEntity> getCustomers() {

        CustomerEntity customerEntity1 = new CustomerEntity("1001", "Rubal");
        CustomerEntity customerEntity2 = new CustomerEntity("1002", "Kishan");

        List<CustomerEntity> customerEntityList = new ArrayList<>();
        customerEntityList.add(customerEntity1);
        customerEntityList.add(customerEntity2);

        return customerEntityList;
    }
}
