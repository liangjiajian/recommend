package dao;


import scala.Serializable;

/************产品历史活动信息***************/
public class ProductsActivity implements Serializable {

    private String  productId;

    private String activityStartDate;

    private String activityEndDate;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getActivityStartDate() {
        return activityStartDate;
    }

    public void setActivityStartDate(String activityStartDate) {
        this.activityStartDate = activityStartDate;
    }

    public String getActivityEndDate() {
        return activityEndDate;
    }

    public void setActivityEndDate(String activityEndDate) {
        this.activityEndDate = activityEndDate;
    }

    @Override
    public String toString() {
        return "ProductsActivity{" +
                "productId='" + productId + '\'' +
                ", activityStartDate='" + activityStartDate + '\'' +
                ", activityEndDate='" + activityEndDate + '\'' +
                '}';
    }
}
