package dao;

import scala.Serializable;

/************用户收藏的产品**********/
public class UserCollectProduct implements Serializable{

    private String userId;

    private String productId;

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

    @Override
    public String toString() {
        return "UserCollectProduct{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                '}';
    }
}
