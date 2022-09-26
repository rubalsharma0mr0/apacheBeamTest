package ApplicationPackage;

public class DisplayProduct {
    public void displayProduct(SearchProduct searchProduct) {

        if (searchProduct.getProductList().contains(searchProduct.getProductName())) {
            System.out.println(searchProduct.getProductName());
        } else {
            System.out.println("Not in the list ");
        }
    }
}
