package service;

import dao.*;

import java.util.List;
import java.util.Map;

public interface DataTreatingService {

    List<ProductsActivity> getProductsActivity();

    List<ProductsPurchasedByUser> getProductsPurchasedByUser();

    List<UserBuyProductsInfo> getUserBuyProducts();

    List<UserCollectProduct> getUserCollectProduct();

    List<UserJoinActivity> getUserJoinActivity();

    List<Map<String,Object>> getUserPurchaseDate();

    String getSystemStartTime();

    List<UserPurchasedProductTime> getUserPurchasedProductTimeInfo();

    List<Map<String,Object>> getUserPurchasedProductQuantity();

    List<Map<String,Object>> getUserId2UserValue();

    List<Map<String,Object>> getProduct2ProductValue();

    List<Map<String,Object>> getUserFirstBuyProduct();

    void svaeRecommendResult(String userId,String product);

}
