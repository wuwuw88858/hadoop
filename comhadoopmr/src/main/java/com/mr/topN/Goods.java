package com.mr.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 18:01
 * @data
 * 13764633023	2014-12-01 02:20:42.000	全视目Allseelook 原宿风暴显色美瞳彩色隐形艺术眼镜1片 拍2包邮	33.6	2	18067781305
 */
public class Goods implements WritableComparable<Goods> {
    private String userId;
    private String datetime;
    private String title;
    private Double unitPrice;
    private int purchaseNum;
    private String productId;

    public Goods() {
    }

    public Goods(String userId, String datetime, String title, Double unitPrice, int purchaseNum, String productId) {
        this.userId = userId;
        this.datetime = datetime;
        this.title = title;
        this.unitPrice = unitPrice;
        this.purchaseNum = purchaseNum;
        this.productId = productId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(Double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public int getPurchaseNum() {
        return purchaseNum;
    }

    public void setPurchaseNum(int purchaseNum) {
        this.purchaseNum = purchaseNum;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }


    /**
     *  以userid升序，相同的话 年月日比较
     * */
    public int compareTo(Goods o) {
        int uderIdCompareResult = this.userId.compareTo(o.userId);

        //相同的用户
        if (uderIdCompareResult == 0) {
             int dateTimeCompareResult = this.datetime.compareTo(o.datetime);
             //相同的时间
             if (dateTimeCompareResult == 0) {
                Double thisTotalPrice = this.unitPrice * this.purchaseNum;
                Double otherTotalPrice = o.unitPrice * o.purchaseNum;
                return -thisTotalPrice.compareTo(otherTotalPrice);  //取反 消耗高的在前
             } else {
                 return dateTimeCompareResult;
             }
        } else {
            return  uderIdCompareResult;
        }
    }

    //序列化
    /**
     *    private String userId;
     private String datetime;
     private String title;
     private Double unitPrice;
     private int purchaseNum;
     private String productId;
     *
     * */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(datetime);
        out.writeUTF(title);
        out.writeDouble(unitPrice);
        out.writeInt(purchaseNum);
        out.writeUTF(productId);
    }

    //反序列化
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.datetime = in.readUTF();
        this.title = in.readUTF();
        this.unitPrice = in.readDouble();
        this.purchaseNum = in.readInt();
        this.productId = in.readUTF();
    }
}
