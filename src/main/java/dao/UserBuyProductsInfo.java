package dao;

import scala.Serializable;

/*************用户购买产品的交易信息************/
public class UserBuyProductsInfo implements Serializable{

    private String userId;

    private String productId;

    private String tradeDate;

    private Double quantity;

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

    public String getTradeDate() {
        return tradeDate;
    }

    public void setTradeDate(String tradeDate) {
        this.tradeDate = tradeDate;
    }

    public Double getQuantity() {
        return quantity;
    }

    public void setQuantity(Double quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "UserBuyProductsInfo{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", tradeDate='" + tradeDate + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
