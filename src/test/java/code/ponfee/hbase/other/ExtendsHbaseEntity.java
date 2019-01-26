package code.ponfee.hbase.other;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import code.ponfee.commons.util.Dates;
import code.ponfee.hbase.Constants;
import code.ponfee.hbase.annotation.HbaseField;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseEntity;

@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "t_test_entity", family = "cf1")
public class ExtendsHbaseEntity extends HbaseEntity<String> {

    private static final long serialVersionUID = -1701075762499122949L;
    private String firstName;
    private String lastName;
    private String nonce;
    private int age;

    @HbaseField(format = "yyyyMMdd")
    @JsonFormat(pattern = "yyyyMMdd")
    private Date birthday;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    @Override
    public String buildRowKey() {
        super.rowKey = firstName + "_" + lastName + "_" + Dates.format(birthday, "yyyyMMdd");
        return super.rowKey;
    }

}
