package service;

import dao.*;
import mybatis.UserMergeProductMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DataTreatingServiceImpl implements DataTreatingService {

    @Autowired
    public UserMergeProductMapper userMergeProductMapper;

    @Override
    public List<ProductsActivity> getProductsActivity() {
        List<ProductsActivity> info = userMergeProductMapper.getProductsActivityInfo();
        return  info;
    }

    @Override
    public List<ProductsPurchasedByUser> getProductsPurchasedByUser() {
        List<ProductsPurchasedByUser> info = userMergeProductMapper.getProductsPurchasedByUserInfo();
        return info;
    }

    @Override
    public List<UserBuyProductsInfo> getUserBuyProducts() {
        List<UserBuyProductsInfo> info = userMergeProductMapper.getUserBuyProductsInfo();
        return  info;
    }

    @Override
    public List<UserCollectProduct> getUserCollectProduct() {
        return null;
    }

    @Override
    public List<UserJoinActivity> getUserJoinActivity() {
        List<UserJoinActivity> info = userMergeProductMapper.getUserJoinActivityInfo();
        return  info;
    }

    @Override
    public List<Map<String, Object>> getUserPurchaseDate() {
        return userMergeProductMapper.getUserPurchaseDate();
    }

    @Override
    public String getSystemStartTime() {
        return userMergeProductMapper.getSystemStartTime();
    }

    @Override
    public List<UserPurchasedProductTime> getUserPurchasedProductTimeInfo() {
        return userMergeProductMapper.getUserPurchasedProductTimeInfo();
    }

    public  List<Map<String,Object>> getUserPurchasedProductQuantity(){
        return userMergeProductMapper.getUserPurchasedProductQuantity();
    }

    @Override
    public List<Map<String, Object>> getUserId2UserValue() {
        return userMergeProductMapper.getUserId2UserValue();
    }

    @Override
    public List<Map<String, Object>> getProduct2ProductValue() {
        return userMergeProductMapper.getProduct2ProductValue();
    }

    @Override
    public List<Map<String, Object>> getUserFirstBuyProduct() {
        return userMergeProductMapper.getUserFirstBuyProduct();
    }


    @Override
    public void svaeRecommendResult(String userId, String product) {
        userMergeProductMapper.svaeRecommendResult(userId,product);
    }
}
