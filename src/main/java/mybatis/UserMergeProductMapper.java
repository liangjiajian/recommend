package mybatis;

import dao.*;

import java.util.List;
import java.util.Map;

public interface UserMergeProductMapper {

     /****获取产品各个月的活动信息***/
     List<ProductsActivity> getProductsActivityInfo();
     /********最近半年交易单个产品超过两个的用户与产品信息********/
     List<ProductsPurchasedByUser> getProductsPurchasedByUserInfo();
     /******用户各个月购买产品的数量*******/
     List<UserBuyProductsInfo> getUserBuyProductsInfo();
     /*******用户收藏某个产品的信息************/
     List<UserCollectProduct> getUserCollectProductInfo();
     /*********用户某月参与活动购买产品信息**********/
     List<UserJoinActivity> getUserJoinActivityInfo();

     /********用户最近半年购买产品的日期*******/
     List<Map<String,Object>> getUserPurchaseDate();

     /***获取系统最初交易时间**/
     String getSystemStartTime();
     /**********用户交易产品的月份**********/
     List<UserPurchasedProductTime> getUserPurchasedProductTimeInfo();
     /********用户近半年购买产品的总订单数与总销售量*******/
     List<Map<String,Object>> getUserPurchasedProductQuantity();

     /**********获取userid对应的userValue用户数据建模（建模数据必须是数值double类型）*********/
     List<Map<String,Object>> getUserId2UserValue();

     /***********获取产品对应的产品productValue************/
     List<Map<String,Object>> getProduct2ProductValue();

     /***********用户第一次购买产品的月份*************/
     List<Map<String,Object>> getUserFirstBuyProduct();

     /*************存储分类推荐结果*************/

     void svaeRecommendResult(String userId,String product);




}
