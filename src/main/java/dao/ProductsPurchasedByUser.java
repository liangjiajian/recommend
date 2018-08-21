package dao;

import scala.Serializable;

/***********用户购买过的产品信息*************/
public class ProductsPurchasedByUser implements Serializable {

    private String userId;

    private String prodcutId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProdcutId() {
        return prodcutId;
    }

    public void setProdcutId(String prodcutId) {
        this.prodcutId = prodcutId;
    }

    @Override
    public String toString() {
        return "ProductsPurchasedByUser{" +
                "userId='" + userId + '\'' +
                ", prodcutId='" + prodcutId + '\'' +
                '}';
    }
}
