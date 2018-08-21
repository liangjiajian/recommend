package dao;

import scala.Serializable;

/**************用户每个月购买产品的第一次时间与最后一次时间****************/
public class UserPurchasedProductTime implements Serializable {

    private String userId;

    private String productId;

    /***用户购买该产品的月份***/
    private String purchasedMoth;
    /****用户当月第一次购买该产品的日期*****/
    private String purchasedStartTime;
    /****用户当月最后一次购买该产品的日期*****/
    private String purchaseEndTime;

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

    public String getPurchasedMoth() {
        return purchasedMoth;
    }

    public void setPurchasedMoth(String purchasedMoth) {
        this.purchasedMoth = purchasedMoth;
    }

    public String getPurchasedStartTime() {
        return purchasedStartTime;
    }

    public void setPurchasedStartTime(String purchasedStartTime) {
        this.purchasedStartTime = purchasedStartTime;
    }

    public String getPurchaseEndTime() {
        return purchaseEndTime;
    }

    public void setPurchaseEndTime(String purchaseEndTime) {
        this.purchaseEndTime = purchaseEndTime;
    }

    @Override
    public String toString() {
        return "UserPurchasedProductTime{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", purchasedMoth='" + purchasedMoth + '\'' +
                ", purchasedStartTime='" + purchasedStartTime + '\'' +
                ", purchaseEndTime='" + purchaseEndTime + '\'' +
                '}';
    }
}
