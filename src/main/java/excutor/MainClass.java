package excutor;

import dao.ProductsActivity;
import dao.ProductsPurchasedByUser;
import dao.UserBuyProductsInfo;
import dao.UserJoinActivity;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import service.DataTreatingServiceImpl;
import util.SparkConfs;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

@Component
public class MainClass{

    private static String applicationPath = "classpath:spring/applicationContext.xml";

    private static ApplicationContext applicationContext = new ClassPathXmlApplicationContext(applicationPath);

//    public  DataTreatingServiceImpl dataTreatingServiceImpl = (DataTreatingServiceImpl) applicationContext.getBean("dataTreatingServiceImpl");

    public static void main(String[] args) {
        String applicationPath = "classpath:spring/applicationContext.xml";

        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(applicationPath);

        DataTreatingServiceImpl dataTreatingServiceImpl = (DataTreatingServiceImpl)
        applicationContext.getBean("dataTreatingServiceImpl");

        List<Map<String,Object>> a =  dataTreatingServiceImpl.getUserPurchasedProductQuantity();
        System.out.println(a.size());
    }


    public DataTreatingServiceImpl getDataTreatingServiceImpl(){

        DataTreatingServiceImpl dataTreatingServiceImpl = (DataTreatingServiceImpl)
                applicationContext.getBean("dataTreatingServiceImpl");
        return dataTreatingServiceImpl;
    }

}