package dao;

import scala.Serializable;

/**********用户参与活动购买产品记录**********/
public class UserJoinActivity implements Serializable{

    private String userId;

    private String productId;

    private String dateOfPurchase;

    private String promotionsId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getDateOfPurchase() {
        return dateOfPurchase;
    }

    public void setDateOfPurchase(String dateOfPurchase) {
        this.dateOfPurchase = dateOfPurchase;
    }

    public String getPromotionsId() {
        return promotionsId;
    }

    public void setPromotionsId(String promotionsId) {
        this.promotionsId = promotionsId;
    }

    @Override
    public String toString() {
        return "UserJoinActivity{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", dateOfPurchase='" + dateOfPurchase + '\'' +
                ", promotionsId='" + promotionsId + '\'' +
                '}';
    }
}