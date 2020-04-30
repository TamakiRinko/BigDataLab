package example;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String oId;                 // 订单ID
    private String oDate;               // 订单日期
    private String pId;                 // 商品ID
    private String pName;               // 商品名称
    private String pPrice;              // 商品单价
    private String oAmount;             // 购买数量

    public OrderBean(){

    }

    public OrderBean(String oId, String oDate, String pId, String pName, String pPrice, String oAmount) {
        this.oId = oId;
        this.oDate = oDate;
        this.pId = pId;
        this.pName = pName;
        this.pPrice = pPrice;
        this.oAmount = oAmount;
    }

    @Override
    public int compareTo(OrderBean o) {
        int compare = this.pId.compareTo(o.pId);
        if(compare == 0){
            // 使得product.txt中的bean排在最前面
            compare = o.pName.compareTo(this.pName);
        }
        return compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(oId);
        dataOutput.writeUTF(oDate);
        dataOutput.writeUTF(pId);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(pPrice);
        dataOutput.writeUTF(oAmount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        oId = dataInput.readUTF();
        oDate = dataInput.readUTF();
        pId = dataInput.readUTF();
        pName = dataInput.readUTF();
        pPrice = dataInput.readUTF();
        oAmount = dataInput.readUTF();
    }

    public String getoId() {
        return oId;
    }

    public void setoId(String oId) {
        this.oId = oId;
    }

    public String getoDate() {
        return oDate;
    }

    public void setoDate(String oDate) {
        this.oDate = oDate;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getpPrice() {
        return pPrice;
    }

    public void setpPrice(String pPrice) {
        this.pPrice = pPrice;
    }

    public String getoAmount() {
        return oAmount;
    }

    public void setoAmount(String oAmount) {
        this.oAmount = oAmount;
    }

    @Override
    public String toString() {
        return oId + '\t' +
                oDate + '\t' +
                pId + '\t' +
                pName + '\t' +
                pPrice + '\t' +
                oAmount;
    }
}
