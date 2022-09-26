package ApplicationPackage;

import java.util.ArrayList;
import java.util.List;

public class SearchProduct {
    private String productName;
    private int productPrice;

    public SearchProduct(String productName, int productPrice) {
        this.productName = productName;
        this.productPrice = productPrice;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
    }

    public List<String> getProductList() {

        List<String> productList = new ArrayList<>();
        productList.add("Jaideep Human");
        productList.add("Naveen Not Human");
        productList.add("Shivankur up for discussion");

        return productList;
    }
}
