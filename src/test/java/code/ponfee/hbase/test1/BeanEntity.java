/* __________              _____                                          *\
** \______   \____   _____/ ____\____   ____        Ponfee's code         **
**  |     ___/  _ \ /    \   __\/ __ \_/ __ \       (c) 2017-2019, MIT    **
**  |    |  (  <_> )   |  \  | \  ___/\  ___/       http://www.ponfee.cn  **
**  |____|   \____/|___|  /__|  \___  >\___  >                            **
**                      \/          \/     \/                             **
\*                                                                        */

package code.ponfee.hbase.test1;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import code.ponfee.commons.collect.ByteArrayWrapper;
import code.ponfee.commons.model.Result;
import code.ponfee.commons.serial.WrappedSerializer;
import code.ponfee.commons.serial.KryoSerializer;
import code.ponfee.commons.util.Dates;
import code.ponfee.commons.util.SecureRandoms;
import code.ponfee.hbase.Constants;
import code.ponfee.hbase.annotation.HbaseField;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseEntity;
import io.netty.util.internal.ThreadLocalRandom;

/**
 * 
 * 
 * @author Ponfee
 */
@HbaseTable(namespace = Constants.HBASE_NAMESPACE, tableName = "t_bean_entity", family = "cf1")
public class BeanEntity extends HbaseEntity<String> {

    private static final long serialVersionUID = 4412422627257733721L;

    private String firstName;
    private String lastName;
    private int age;

    @HbaseField(format = HbaseField.FORMAT_TIMESTAMP)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date tstamp = Dates.random(Dates.toDate("2000-01-01 00:00:00"), new Date());

    private long long1 = ThreadLocalRandom.current().nextLong();
    private int int1 = ThreadLocalRandom.current().nextInt();
    private float float1 = ThreadLocalRandom.current().nextFloat();
    private double double1 = ThreadLocalRandom.current().nextDouble();
    private boolean bool1 = true;
    private boolean bool2 = false;
    private byte byte1 = 123;
    private short short1 = 456;
    private char char1 = 'x';

    @HbaseField(family = "cf2", qualifier = "long_2")
    private Long long2 = ThreadLocalRandom.current().nextLong();

    @HbaseField(family = "cf2", qualifier = "long_3")
    private Long long3;

    @HbaseField(family = "cf2", qualifier = "")
    private Integer int2 = ThreadLocalRandom.current().nextInt();

    @HbaseField(family = "cf2", qualifier = "int_3")
    private Integer int3;

    @HbaseField(family = "cf2")
    private Float float2 = ThreadLocalRandom.current().nextFloat();

    @HbaseField(serializer = WrappedSerializer.class)
    private Float float3;

    private Double double2 = ThreadLocalRandom.current().nextDouble();

    private Double double3;

    private Boolean bool3 = true;

    private Boolean bool4;

    private Byte byte2 = 11;

    private Byte byte3;

    private Short short2 = 22;

    private Short short3;

    private Character char2 = 'c';

    private Character char3;

    private ByteArrayWrapper baw = ByteArrayWrapper.of(SecureRandoms.nextBytes(5));

    @HbaseField(serializer = KryoSerializer.class)
    private Result<String> res = Result.of(12, "test");

    @HbaseField(format = "yyyyMMdd")
    @JsonFormat(pattern = "yyyyMMdd", timezone = "GMT+8")
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

    @Override
    public String buildRowKey() {
        super.rowKey = firstName + "_" + lastName + "_" + Dates.format(birthday, "yyyyMMdd");
        return super.rowKey;
    }

    public Date getTstamp() {
        return tstamp;
    }

    public void setTstamp(Date tstamp) {
        this.tstamp = tstamp;
    }

    public long getLong1() {
        return long1;
    }

    public void setLong1(long long1) {
        this.long1 = long1;
    }

    public int getInt1() {
        return int1;
    }

    public void setInt1(int int1) {
        this.int1 = int1;
    }

    public float getFloat1() {
        return float1;
    }

    public void setFloat1(float float1) {
        this.float1 = float1;
    }

    public double getDouble1() {
        return double1;
    }

    public void setDouble1(double double1) {
        this.double1 = double1;
    }

    public boolean isBool1() {
        return bool1;
    }

    public void setBool1(boolean bool1) {
        this.bool1 = bool1;
    }

    public boolean isBool2() {
        return bool2;
    }

    public void setBool2(boolean bool2) {
        this.bool2 = bool2;
    }

    public byte getByte1() {
        return byte1;
    }

    public void setByte1(byte byte1) {
        this.byte1 = byte1;
    }

    public short getShort1() {
        return short1;
    }

    public void setShort1(short short1) {
        this.short1 = short1;
    }

    public char getChar1() {
        return char1;
    }

    public void setChar1(char char1) {
        this.char1 = char1;
    }

    public Long getLong2() {
        return long2;
    }

    public void setLong2(Long long2) {
        this.long2 = long2;
    }

    public Long getLong3() {
        return long3;
    }

    public void setLong3(Long long3) {
        this.long3 = long3;
    }

    public Integer getInt2() {
        return int2;
    }

    public void setInt2(Integer int2) {
        this.int2 = int2;
    }

    public Integer getInt3() {
        return int3;
    }

    public void setInt3(Integer int3) {
        this.int3 = int3;
    }

    public Float getFloat2() {
        return float2;
    }

    public void setFloat2(Float float2) {
        this.float2 = float2;
    }

    public Float getFloat3() {
        return float3;
    }

    public void setFloat3(Float float3) {
        this.float3 = float3;
    }

    public Double getDouble2() {
        return double2;
    }

    public void setDouble2(Double double2) {
        this.double2 = double2;
    }

    public Double getDouble3() {
        return double3;
    }

    public void setDouble3(Double double3) {
        this.double3 = double3;
    }

    public Boolean getBool3() {
        return bool3;
    }

    public void setBool3(Boolean bool3) {
        this.bool3 = bool3;
    }

    public Boolean getBool4() {
        return bool4;
    }

    public void setBool4(Boolean bool4) {
        this.bool4 = bool4;
    }

    public Byte getByte2() {
        return byte2;
    }

    public void setByte2(Byte byte2) {
        this.byte2 = byte2;
    }

    public Byte getByte3() {
        return byte3;
    }

    public void setByte3(Byte byte3) {
        this.byte3 = byte3;
    }

    public Short getShort2() {
        return short2;
    }

    public void setShort2(Short short2) {
        this.short2 = short2;
    }

    public Short getShort3() {
        return short3;
    }

    public void setShort3(Short short3) {
        this.short3 = short3;
    }

    public Character getChar2() {
        return char2;
    }

    public void setChar2(Character char2) {
        this.char2 = char2;
    }

    public Character getChar3() {
        return char3;
    }

    public void setChar3(Character char3) {
        this.char3 = char3;
    }

    public ByteArrayWrapper getBaw() {
        return baw;
    }

    public void setBaw(ByteArrayWrapper baw) {
        this.baw = baw;
    }

    public Result<String> getRes() {
        return res;
    }

    public void setRes(Result<String> res) {
        this.res = res;
    }

}
