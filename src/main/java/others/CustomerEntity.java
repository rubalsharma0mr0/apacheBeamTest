package others;

import java.io.Serializable;

//Create PCollection object from JAVA class

//Note: In Big Data all data needs to be serialized as object will be processed on the distributed nodes

/*
To serialize an object means to convert its state to a byte stream so that the byte stream can be reverted back
into a copy of the object. A Java object is serializable if its class or any of its superclasses
implements either the java. io. Serializable interface or its sub-interface, java.
*/
public class CustomerEntity implements Serializable {
    private String id;
    private String name;

    public CustomerEntity() {

    }

    public CustomerEntity(String pID, String pName) {
        this.id = pID;
        this.name = pName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}