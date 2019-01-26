package code.ponfee.hbase.other;

import static code.ponfee.hbase.HbaseHelper.partition;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import code.ponfee.commons.util.Dates;
import code.ponfee.hbase.Constants;
import code.ponfee.hbase.annotation.HbaseField;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseEntity;

@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "t_bas_order_info", family="cf1")
public class BasOrderInfo extends HbaseEntity<String> {

    private static final String DATE_PATTERN1 = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String DATE_PATTERN2 = "yyyy-MM-dd HH:mm:ss";

    // {"first_loading_tm":"NULL","warehouse_code":"010DCF","city_code":"532","closed_time":"2016-04-02 14:31:28.0","product_code":"SE0004","actual_weight":"0.51","order_date":"2016-04-01 20:30:48.0","city_name":"青岛市","warehouse_name":"北京通州RDC","sku_no":"03.21.3211102-T","sku_qty":"1.0","price":"799.0","wms_receive_time":"2016-04-02 11:11:46.0","rowNum":0,"order_amount":"799.0","inc_day":"20160401","rowkey":"4_MEIZU_20160401_S1603290008630_03.21.3211102-T","waybill_no":"NULL","company_code":"MEIZU","signin_tm":"2016-04-04 17:12:39.0","rowKey":"4_MEIZU_20160401_S1603290008630_03.21.3211102-T","erp_order":"S1603290008630","is_other_warehouse":"0"}
    private static final long serialVersionUID = -913024633554782398L;

    //@HbaseField(qualifier = "first_loading_tm")
    private String firstLoadingTm;

    //@HbaseField(qualifier = "warehouse_code")
    private String warehouseCode;

    //@HbaseField(qualifier = "city_code")
    private String cityCode;

    @HbaseField(format = { DATE_PATTERN1, DATE_PATTERN2 })
    @JsonFormat(pattern=DATE_PATTERN2)
    private Date closedTime;

    //@HbaseField(qualifier = "product_code")
    private String productCode;

    //@HbaseField(qualifier = "actual_weight")
    private double actualWeight;

    @HbaseField(format = { DATE_PATTERN1, DATE_PATTERN2 })
    @JsonFormat(pattern=DATE_PATTERN2)
    private Date orderDate;

    //@HbaseField(qualifier = "city_name")
    private String cityName;

    //@HbaseField(qualifier = "warehouse_name")
    private String warehouseName;

    //@HbaseField(qualifier = "sku_no")
    private String skuNo;

    //@HbaseField(qualifier = "sku_qty")
    private long skuQty;

    //@HbaseField(qualifier = "price")
    private double price;

    @HbaseField(format = { DATE_PATTERN1, DATE_PATTERN2 })
    @JsonFormat(pattern=DATE_PATTERN2)
    private Date wmsReceiveTime;

    //@HbaseField(qualifier = "order_amount")
    private double orderAmount;

    //@HbaseField(qualifier = "inc_day")
    private String incDay;

    //@HbaseField(qualifier = "waybill_no")
    private String waybillNo;

    //@HbaseField(qualifier = "company_code")
    private String companyCode;

    @HbaseField(format = { DATE_PATTERN1, DATE_PATTERN2 })
    @JsonFormat(pattern=DATE_PATTERN2)
    private Date signinTm;

    //@HbaseField(qualifier = "erp_order")
    private String erpOrder;

    //@HbaseField(qualifier = "is_other_warehouse")
    private String isOtherWarehouse;

    @Override
    public String buildRowKey() {
        //4_MEIZU_20160401_S1603290008630_03.21.3211102-T
        return super.rowKey = String.join("_", 
           partition(companyCode), warehouseCode, 
           Dates.format(orderDate, "yyyyMMdd"), erpOrder, skuNo
       );
    }

    public String getFirstLoadingTm() {
        return firstLoadingTm;
    }

    public void setFirstLoadingTm(String firstLoadingTm) {
        this.firstLoadingTm = firstLoadingTm;
    }

    public String getWarehouseCode() {
        return warehouseCode;
    }

    public void setWarehouseCode(String warehouseCode) {
        this.warehouseCode = warehouseCode;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode;
    }

    public Date getClosedTime() {
        return closedTime;
    }

    public void setClosedTime(Date closedTime) {
        this.closedTime = closedTime;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public double getActualWeight() {
        return actualWeight;
    }

    public void setActualWeight(double actualWeight) {
        this.actualWeight = actualWeight;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public void setWarehouseName(String warehouseName) {
        this.warehouseName = warehouseName;
    }

    public String getSkuNo() {
        return skuNo;
    }

    public void setSkuNo(String skuNo) {
        this.skuNo = skuNo;
    }

    public long getSkuQty() {
        return skuQty;
    }

    public void setSkuQty(long skuQty) {
        this.skuQty = skuQty;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Date getWmsReceiveTime() {
        return wmsReceiveTime;
    }

    public void setWmsReceiveTime(Date wmsReceiveTime) {
        this.wmsReceiveTime = wmsReceiveTime;
    }

    public double getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(double orderAmount) {
        this.orderAmount = orderAmount;
    }

    public String getIncDay() {
        return incDay;
    }

    public void setIncDay(String incDay) {
        this.incDay = incDay;
    }

    public String getWaybillNo() {
        return waybillNo;
    }

    public void setWaybillNo(String waybillNo) {
        this.waybillNo = waybillNo;
    }

    public String getCompanyCode() {
        return companyCode;
    }

    public void setCompanyCode(String companyCode) {
        this.companyCode = companyCode;
    }

    public Date getSigninTm() {
        return signinTm;
    }

    public void setSigninTm(Date signinTm) {
        this.signinTm = signinTm;
    }

    public String getErpOrder() {
        return erpOrder;
    }

    public void setErpOrder(String erpOrder) {
        this.erpOrder = erpOrder;
    }

    public String getIsOtherWarehouse() {
        return isOtherWarehouse;
    }

    public void setIsOtherWarehouse(String isOtherWarehouse) {
        this.isOtherWarehouse = isOtherWarehouse;
    }

}
