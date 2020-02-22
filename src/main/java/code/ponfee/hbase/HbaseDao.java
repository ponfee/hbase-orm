package code.ponfee.hbase;

import static code.ponfee.hbase.HbaseUtils.fromBytes;
import static code.ponfee.hbase.HbaseUtils.toBytes;
import static code.ponfee.hbase.model.HbaseMap.ROW_KEY_NAME;
import static code.ponfee.hbase.model.HbaseMap.ROW_NUM_NAME;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import code.ponfee.commons.cache.TimestampProvider;
import code.ponfee.commons.io.Closeables;
import code.ponfee.commons.model.SortOrder;
import code.ponfee.commons.reflect.BeanMaps;
import code.ponfee.commons.reflect.CglibUtils;
import code.ponfee.commons.reflect.ClassUtils;
import code.ponfee.commons.reflect.Fields;
import code.ponfee.commons.reflect.GenericUtils;
import code.ponfee.commons.serial.NullSerializer;
import code.ponfee.commons.serial.Serializer;
import code.ponfee.commons.util.Bytes;
import code.ponfee.commons.util.ObjectUtils;
import code.ponfee.hbase.annotation.HbaseField;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.Column;
import code.ponfee.hbase.model.HbaseBean;
import code.ponfee.hbase.model.HbaseEntity;
import code.ponfee.hbase.model.HbaseMap;
import code.ponfee.hbase.model.PageQueryBuilder;

/**
 * The Hbase dao common base class
 * 
 * @author Ponfee
 * @param <T> the HbaseBean(HbaseEntity or HbaseMap)
 * @param <R> the hbase rowkey of HbaseBean(HbaseEntity or HbaseMap)
 */
public abstract class HbaseDao<T extends HbaseBean<R>, R extends Serializable & Comparable<? super R>> {

    private static Logger logger = LoggerFactory.getLogger(HbaseDao.class);

    private static final Map<Class<? extends Serializer>, Serializer> REGISTERED_SERIALIZER = 
        new /*ConcurrentHashMap*/HashMap<>();

    //private static final TimestampProvider PROVIDER = TimestampProvider.CURRENT;
    private static final TimestampProvider PROVIDER = TimestampProvider.LATEST;

    private final Class<T> beanType;
    private final RowMapper<T> rowMapper;
    private final Class<R> rowKeyType;
    private final ImmutableMap<String, Column> columnMap;
    private final String globalFamily; // table(class)-level family name
    private final byte[] globalFamilyBytes;
    private final List<byte[]> definedFamiliesBytes;
    private final Serializer rowkeySerializer;
    protected final String tableName;

    protected @Resource HbaseTemplate template;

    @SuppressWarnings("unchecked")
    public HbaseDao() {
        // 1、Get the bean type of ORM's O(object)
        Class<?> clazz = this.getClass();
        this.beanType = GenericUtils.getActualTypeArgument(clazz, 0);
        if (   !HbaseEntity.class.isAssignableFrom(this.beanType)
            && !HbaseMap.class.isAssignableFrom(this.beanType)
        ) {
            throw new UnsupportedOperationException(
                "The class generic type must be HbaseEntity or HbaseMap"
            );
        }

        // 2、Gets the hbase row key type
        this.rowKeyType = GenericUtils.getActualTypeArgument(clazz, 1);

        // 3、Table name, if not defined in annotation then 
        // defaults the class name lower underscore name
        HbaseTable ht = this.beanType.getDeclaredAnnotation(HbaseTable.class);
        String tableName = (ht == null || isBlank(ht.tableName())) 
                           ? toLowerCamel(clazz.getSimpleName())
                           : ht.tableName().trim();
        this.tableName = buildTableName(ht != null ? ht.namespace() : "", tableName);
        this.rowkeySerializer = (ht != null) ? getSerializer(ht.serializer()) : null;

        // 4、Global family and all configed column families
        this.globalFamily = (ht == null || isBlank(ht.family())) ? null : ht.family().trim();
        this.globalFamilyBytes = isEmpty(this.globalFamily) ? null : this.globalFamily.getBytes();
        Set<String> configedFamilies = Sets.newHashSet(); // prevent duplicate: configed global and Fields column family
        ImmutableList.Builder<byte[]> familiesBuilder = new ImmutableList.Builder<>();
        if (this.globalFamilyBytes != null) {
            configedFamilies.add(this.globalFamily);
            familiesBuilder.add(this.globalFamilyBytes);
        }

        // 5、Gets the field(hbase column) configuration
        ImmutableMap.Builder<String, Column> columnsBuilder = new ImmutableMap.Builder<>();
        for (Field f : ClassUtils.listFields(this.beanType)) {
            HbaseField hf = f.getAnnotation(HbaseField.class);
            if (hf != null && hf.ignore()) {
                continue;
            }
            String family = Optional.ofNullable(hf).map(HbaseField::family)
                                    .filter(StringUtils::isNotBlank)
                                    .orElse(this.globalFamily);
            if (isBlank(family)) {
                throw new RuntimeException("Unconfiged column family, bean filed: " + f.getName());
            }
            family = family.trim();
            if (configedFamilies.add(family)) {
                familiesBuilder.add(toBytes(family));
            }

            String qualifier = Optional.ofNullable(hf).map(HbaseField::qualifier)
                                       .filter(StringUtils::isNotBlank)
                                       .orElse(toLowerCamel(f.getName())).trim();
            Serializer serializer = (hf == null) ? null : getSerializer(hf.serializer());
            String[] format = hf == null ? null : hf.format();
            columnsBuilder.put(qualifier, new Column(f, family, qualifier, serializer, format));
        }
        this.columnMap = columnsBuilder.build();
        this.definedFamiliesBytes = familiesBuilder.build();

        // 6、Defined the java bean & hbase row mapping
        this.rowMapper = (result, rowNum) -> {
            if (result.isEmpty()) {
                return null;
            }
            T bean = ObjectUtils.newInstance(this.beanType);
            bean.setRowKey(deserialRowKey(result.getRow()));
            if (bean instanceof HbaseEntity) {
                result.listCells().forEach(cell -> {
                    // CellUtil.cloneFamily(cell), cell.getTimestamp(), cell.getSequenceId()
                    String qualifier = Bytes.toString(cloneQualifier(cell));
                    Column column = this.columnMap.get(qualifier);
                    Object value = deserialValue(column, cloneValue(cell));
                    if (value != null) {
                        Fields.put(bean, column.getField(), value);
                    }
                });
            } else if (bean instanceof HbaseMap) {
                Map<String, Object> map = (Map<String, Object>) bean;
                // HbaseMap only support string value
                result.listCells().forEach(cell -> map.put(
                    Bytes.toString(cloneQualifier(cell)), Bytes.toString(cloneValue(cell))
                ));
            } else {
                throw new UnsupportedOperationException(
                    "Unsupported type: " + this.beanType.getCanonicalName()
                );
            }
            return bean;
        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <F> T convert(F from, Consumer<T> action) {
        T to;
        if (this.beanType.isInstance(from)) {
            to = (T) from;
        } else {
            to = ObjectUtils.newInstance(this.beanType);
            if (Map.class.isAssignableFrom(this.beanType)) {
                if (Map.class.isInstance(from)) {
                    ((Map) to).putAll((Map<?, ?>) from);
                } else {
                    ((Map) to).putAll(BeanMaps.CGLIB.toMap(from));
                }
            } else if (Map.class.isInstance(from)) {
                BeanMaps.CGLIB.copyFromMap((Map) from, to);
            } else {
                CglibUtils.copyProperties(from, to);
            }
        }
        if (action != null) {
            action.accept(to);
        }
        return to;
    }

    public final <F> List<T> convert(List<F> from, Consumer<T> action) {
        if (from == null) {
            return null;
        } else if (from.isEmpty()) {
            return Collections.emptyList();
        }

        return from.stream()
                   .map(f -> convert(f, action))
                   .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------config and connection
    public final Configuration getConfig() {
        return template.getConfiguration();
    }

    public final Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(getConfig());
    }

    public final Table getTable(String tableName) throws IOException {
        return getConnection().getTable(TableName.valueOf(tableName));
    }

    public final void closeConnection(Connection conn) {
        Closeables.log(conn, "Close hbase connection occur error.");
    }

    public final void closeTable(Table table) {
        Closeables.log(table, "Close hbase table occur error.");
    }

    // ------------------------------------------------------------------admin operations
    public boolean tableExists() {
        return tableExists(null, this.tableName);
    }

    /**
     * Returns the hbase table exists
     * 
     * @param namespace the namespace
     * @param tableName the table name
     * @return if {@code true} that table exists
     */
    public boolean tableExists(String namespace, String tableName) {
        Preconditions.checkArgument(isNotEmpty(tableName));
        try (Connection conn = getConnection();
             Admin admin = conn.getAdmin()
        ) {
            return admin.tableExists(TableName.valueOf(buildTableName(namespace, tableName)));
        } catch (IOException e) {
            logger.error("Checks hbase table exists {}:{} occur error.", namespace, tableName, e);
            return false;
        }
    }

    public boolean createTable() {
        // this.tableName include namespace
        return createTable(null, this.tableName, this.definedFamiliesBytes);
    }

    public boolean createTable(String tableName, String[] colFamilies) {
        return createTable(null, tableName, colFamilies);
    }

    public boolean createTable(String namespace, String tableName, String[] colFamilies) {
        List<byte[]> families = Stream.of(colFamilies).map(HbaseUtils::toBytes)
                                      .collect(Collectors.toList());
        return createTable(namespace, tableName, families);
    }

    /**
     * Returns create hbase table result, if return {@code true}
     * means create success
     * 
     * @param namespace   the hbase namespace, if null then use hbase default
     * @param tableName   the hbase table name
     * @param colFamilies the hbase column families
     * @return if create success then return {@code true}
     */
    public boolean createTable(String namespace, String tableName, 
                               @Nonnull List<byte[]> colFamilies) {
        Preconditions.checkArgument(isNotEmpty(tableName));
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(colFamilies));
        try (Connection conn = getConnection();
             Admin admin = conn.getAdmin()
        ) {
            String namespace0 = isEmpty(namespace) && tableName.indexOf(':') != -1 
                                ? substringBefore(tableName, ":") : namespace;
            if (   isNotBlank(namespace0) 
                && Stream.of(admin.listNamespaceDescriptors())
                         .noneMatch(nd -> nd.getName().equals(namespace0))
            ) {
                // 创建表空间
                admin.createNamespace(NamespaceDescriptor.create(namespace0).build());
            }
            TableName table = TableName.valueOf(buildTableName(namespace, tableName));
            if (admin.tableExists(table)) {
                logger.warn("Hbase table {}:{} exists.", namespace, tableName);
                return false;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(table);
            //tableDesc.setDurability(Durability.USE_DEFAULT); // 设置为默认的Write-Ahead-Log级别
            colFamilies.forEach(family -> {
                HColumnDescriptor hcd = new HColumnDescriptor(family);
                //hcd.setTimeToLive(5184000); // 设置数据保存的最长时间
                //hcd.setMaxVersions(1); // 设置数据保存的最大版本数
                //hcd.setInMemory(true); // 设置数据保存在内存中以提高响应速度
                tableDesc.addFamily(hcd);
            });
            admin.createTable(tableDesc);
            return true;
        } catch (IOException e) {
            logger.error("Create hbase table {}:{} occur error.", namespace, tableName, e);
            return false;
        }
    }

    public List<String> descTable() {
        // this.tableName include namespace
        return descTable(null, this.tableName);
    }

    public List<String> descTable(String tableName) {
        return descTable(null, tableName);
    }

    /**
     * Returns the hbase table column family info
     * 
     * @param namespace the hbase namespace, if null then use hbase default 
     * @param tableName the hbase table name
     * @return a list string of column families info
     */
    public List<String> descTable(String namespace, String tableName) {
        tableName = buildTableName(namespace, tableName);
        return template.execute(tableName, table -> {
            HTableDescriptor desc = table.getTableDescriptor();
            List<String> result = new LinkedList<>();
            for (HColumnDescriptor hcd : desc.getColumnFamilies()) {
                result.add(hcd.toString());
            }
            return result;
        });
    }

    public boolean dropTable() {
        // this.tableName include namespace
        return dropTable(null, this.tableName);
    }

    public boolean dropTable(String tableName) {
        return dropTable(null, tableName);
    }

    /**
     * Returns drop hbase table result
     * 
     * @param namespace the hbase namespace, if null then use hbase default 
     * @param tableName the hbase table name
     * @return if {@code true} means drop success
     */
    public boolean dropTable(String namespace, String tableName) {
        try (Connection conn = getConnection();
             Admin admin = conn.getAdmin()
        ) {
            TableName table = TableName.valueOf(buildTableName(namespace, tableName));
            if (!admin.tableExists(table)) {
                logger.warn("Hbase table {}:{} not exists.", namespace, tableName);
                return false;
            }
            admin.disableTable(table);
            admin.deleteTable(table);
            return true;
        } catch (IOException e) {
            logger.error("Drop hbase table {}:{} occur error.", namespace, tableName, e);
            return false;
        }
    }

    // ------------------------------------------------------------------get by row key
    public T get(String rowKey) {
        return get(rowKey, globalFamily, null);
    }

    public T get(String rowKey, String familyName) {
        return get(rowKey, familyName, null);
    }

    /**
     * Returns a hbase row data spec rowKey
     * 
     * @param rowKey the hbase row key
     * @param familyName the hbase column family name 
     * @param qualifier then hbase column qualifier
     * @return a hbase row data
     */
    public T get(String rowKey, String familyName, String qualifier) {
        return template.get(tableName, rowKey, familyName, 
                            qualifier, rowMapper);
    }

    // ------------------------------------------------------------------find data list
    public List<T> find(String startRow, int pageSize) {
        return find(startRow, null, pageSize, false);
    }

    public List<T> find(String startRow, String stopRow, int pageSize) {
        return find(startRow, stopRow, pageSize, false);
    }

    /**
     * Returns hbase row data list, where condition is 
     * rowkey between startRow and stopRow,
     * if pageSize < 1 then return all condition data 
     * else maximum returns pageSize data list
     * 
     * @param startRow  start row key
     * @param stopRow   stop row key
     * @param pageSize page size
     * @param reversed the reversed order, if {@code true} then startRow > stopRow 
     *                 eg: <code>find("name94", "name89", 20, true)</code>
     * @return hbase row data list
     */
    public List<T> find(String startRow, String stopRow, int pageSize, boolean reversed) {
        return find(startRow, stopRow, pageSize, reversed, scan -> {
            addDefinedFamilies(scan);
            // include stop row
            if (isNotEmpty(stopRow)) {
                scan.setFilter(new InclusiveStopFilter(toBytes(stopRow)));
            }
        }, true);
    }

    // ------------------------------------------------------------------rang with start and stop row key
    public List<T> range(String startRow, String stopRow) {
        return find(startRow, stopRow, 0, false);
    }

    public List<T> all() {
        return find(null, null, 0, false);
    }

    // ------------------------------------------------------------------find by row key prefix
    public List<T> prefix(String rowKeyPrefix) {
        return prefix(rowKeyPrefix, null, 0);
    }

    public List<T> prefix(String rowKeyPrefix, String startRow) {
        return prefix(rowKeyPrefix, startRow, 0);
    }

    public List<T> prefix(String rowKeyPrefix, int pageSize) {
        return prefix(rowKeyPrefix, null, pageSize);
    }

    /**
     * Returns hbase row data list, where condition is rowkey begin startRow,
     * and rowkey must prefix in rowKeyPrefix
     * if pageSize < 1 then return all condition data 
     * else maximum returns pageSize data list
     * 
     * @param rowKeyPrefix the row key prefix
     * @param startRow  the start row
     * @param pageSize  the page size
     * @return hbase row data list
     */
    public List<T> prefix(String rowKeyPrefix, String startRow, int pageSize) {
        return find(startRow, null, pageSize, false, scan -> {
            addDefinedFamilies(scan);
            scan.setFilter(new PrefixFilter(toBytes(rowKeyPrefix)));
        }, true);
    }

    // ------------------------------------------------------------------find by row key regexp
    public List<T> regexp(String rowKeyRegexp) {
        return regexp(rowKeyRegexp, null, 0);
    }

    public List<T> regexp(String rowKeyRegexp, String startRow) {
        return regexp(rowKeyRegexp, startRow, 0);
    }

    public List<T> regexp(String rowKeyRegexp, int pageSize) {
        return regexp(rowKeyRegexp, null, pageSize);
    }

    /**
     * Returns hbase row data list, where condition is rowkey begin startRow,
     * and rowkey with match rowKeyRegexp pattern
     * if pageSize < 1 then return all condition data 
     * else maximum returns pageSize data list
     * 
     * @param rowKeyRegexp the row key regexp pattern, eg: "^name.*1$"
     * @param startRow  the start row
     * @param pageSize  the page size
     * @return hbase row data list
     */
    public List<T> regexp(String rowKeyRegexp, String startRow, int pageSize) {
        return find(startRow, null, pageSize, false, scan -> {
            addDefinedFamilies(scan);
            RegexStringComparator regexp = new RegexStringComparator(rowKeyRegexp);
            scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, regexp));
        }, true);
    }

    // ------------------------------------------------------------------find for page
    public List<T> nextPage(PageQueryBuilder query) {
        return page(query, true, query.sortOrder() != SortOrder.ASC);
    }

    public List<T> previousPage(PageQueryBuilder query) {
        return page(query, false, query.sortOrder() == SortOrder.ASC);
    }

    // ------------------------------------------------------------------get the last|first row
    public T first() {
        List<T> result = find(null, null, 1, false);
        return CollectionUtils.isEmpty(result) ? null : result.get(0);
    }

    public T last() {
        List<T> result = find(null, null, 1, true);
        return CollectionUtils.isEmpty(result) ? null : result.get(0);
    }

    /**
     * Gets the next rowkey from start rowkey
     * 
     * @param rowKeyPrefix the key prefix
     * @param startRowKey the start rowkey
     * @return a next rowkey relatively start rowkey
     */
    public String nextRowKey(String rowKeyPrefix, String startRowKey) {
        return (String) nearRowKey(rowKeyPrefix, startRowKey, true);
    }

    /**
     * Gets the previous rowkey from start rowkey within rowkey prefix
     * 
     * @param rowKeyPrefix the key prefix
     * @param startRowKey the start rowkey
     * @return a previous rowkey relatively start rowkey
     */
    public String previousRowKey(String rowKeyPrefix, String startRowKey) {
        return (String) nearRowKey(rowKeyPrefix, startRowKey, false);
    }

    /**
     * Gets the max rowkey from start rowkey within rowkey prefix
     * 
     * @param rowKeyPrefix the key prefix
     * @param startRowKey the start rowkey
     * @param paddingLength appending length start rowkey with 0xff
     * @return a previous rowkey relatively start rowkey
     */
    public String maxRowKey(String rowKeyPrefix, String startRowKey, int paddingLength) {
        byte[] startRowKeyBytes = HbaseUtils.paddingStopRowKey(startRowKey, paddingLength);
        Object rowKey = nearRowKey(rowKeyPrefix, startRowKeyBytes, false);
        return (rowKey instanceof String) ? (String) rowKey : startRowKey;
    }

    // ------------------------------------------------------------------put value into hbase
    public boolean put(String tableName, String rowKey, String familyName,
                       String qualifier, String value) {
        return template.execute(tableName, table -> {
            Put put = new Put(toBytes(rowKey));
            put.addColumn(
                toBytes(familyName), toBytes(qualifier), PROVIDER.get(), toBytes(value)
            );
            table.put(put);
            return true;
        });
    }

    public boolean put(String tableName, String rowKey, String familyName,
                       String[] qualifiers, Object[] values) {
        return template.execute(tableName, table -> {
            Put put = new Put(toBytes(rowKey));
            byte[] family = toBytes(familyName);
            long ts = PROVIDER.get();
            for (int n = qualifiers.length, i = 0; i < n; i++) {
                put.addColumn(
                    family, toBytes(qualifiers[i]), ts, toBytes(values[i])
                );
            }
            table.put(put);
            return true;
        });
    }

    // ------------------------------------------------------------------put one row data into hbase
    public boolean put(String tableName, String rowKey, 
                       String familyName, Map<String, Object> data) {
        Preconditions.checkArgument(isNotEmpty(rowKey));
        return template.execute(tableName, table -> {
            Put put = buildPut(toBytes(rowKey), toBytes(familyName), PROVIDER.get(), data);
            if (put.isEmpty()) {
                logger.warn("Empty put data.");
                return false;
            } else {
                table.put(put);
                return true;
            }
        });
    }

    // ------------------------------------------------------------------batch put row data into hbase
    public boolean put(String tableName, String familyName, Map<String, Object> data) {
        return put(tableName, familyName, Collections.singletonList(data));
    }

    public boolean put(String tableName, String familyName, List<Map<String, Object>> list) {
        return template.execute(tableName, table -> {
            List<Put> batch = new ArrayList<>(list.size());
            byte[] family = toBytes(familyName);
            long ts = PROVIDER.get(); Object rowKey;
            for (Map<String, Object> data : list) {
                if ((rowKey = data.get(ROW_KEY_NAME)) == null) {
                    throw new UnsupportedOperationException(
                        "The map data must a key name of 'rowKey' mapping hbase row key."
                    );
                }
                Put put = buildPut(toBytes(rowKey.toString()), family, ts, data);
                if (!batch.isEmpty()) {
                    batch.add(put);
                }
            }
            if (batch.isEmpty()) {
                logger.warn("Empty batch put.");
                return false;
            } else {
                table.batch(batch, new Object[batch.size()]); // table.put(batch);
                return true;
            }
        });
    }

    // ------------------------------------------------------------------put batch data into hbase
    public boolean put(List<T> data) {
        return put(null, data);
    }

    @SuppressWarnings("unchecked")
    public <V> boolean put(String familyName, List<T> data) {
        if (CollectionUtils.isEmpty(data)) {
            return false;
        }

        return template.execute(tableName, table -> {
            List<Put> batch = new ArrayList<>(data.size());
            long ts = PROVIDER.get(); byte[] fam = toBytes(familyName), rowKey;
            for (T obj : data) {
                if (ArrayUtils.isEmpty(rowKey = serialRowKey(obj.getRowKey()))) {
                    throw new UnsupportedOperationException(
                        "Cannot get rowKey data or rowKey data is empty."
                    );
                }

                Put put;
                if (obj instanceof HbaseEntity) {
                    put = new Put(rowKey);
                    HbaseEntity<R> entity = (HbaseEntity<R>) obj;
                    this.columnMap.forEach((name, column) -> {
                        byte[] value = serialValue(entity, column);
                        if (value != null) {
                            byte[] family = defaultIfEmpty(fam, column.getFamilyBytes());
                            put.addColumn(family, column.getQualifierBytes(), ts, value);
                        }
                    });
                } else if (obj instanceof HbaseMap) {
                    byte[] faimly = defaultIfEmpty(fam, this.globalFamilyBytes);
                    if (faimly == null) {
                        throw new IllegalArgumentException("Family cannot be null.");
                    }
                    put = buildPut(rowKey, faimly, ts, (Map<String, Object>) obj);
                } else {
                    throw new UnsupportedOperationException(
                        "Unsupported type: " + beanType.getCanonicalName()
                    );
                }
                if (!put.isEmpty()) {
                    batch.add(put);
                }
            }

            if (batch.isEmpty()) {
                logger.warn("Empty batch put.");
                return false;
            } else {
                table.batch(batch, new Object[batch.size()]);
                return true;
            }
        });
    }

    // ------------------------------------------------------------------delete data from hbase spec rowkey
    public boolean delete(List<String> rowKeys) {
        return delete(tableName, rowKeys, null);
    }

    public boolean deleteFamily(List<String> rowKeys) {
        return delete(tableName, rowKeys, this.definedFamiliesBytes);
    }

    public boolean delete(String tableName, List<String> rowKeys) {
        return delete(tableName, rowKeys, null);
    }

    public boolean delete(String tableName, List<String> rowKeys, List<byte[]> families) {
        return template.execute(tableName, table -> {
            List<Delete> batch = new ArrayList<>(rowKeys.size());
            for (String rowKey : rowKeys) {
                Delete delete = new Delete(toBytes(rowKey));
                if (families != null) {
                    families.forEach(delete::addFamily);
                }
                batch.add(delete);
            }
            table.batch(batch, new Object[batch.size()]);
            return true;
        });
    }

    // ------------------------------------------------------------------protected methods
    protected Scan buildScan(Object startRow, Object stopRow, int pageSize,
                             boolean reversed, ScanHook scanHook) {
        Scan scan = new Scan();
        scan.setReversed(reversed);
        byte[] startRowBytes = toBytes(startRow);
        if (ArrayUtils.isNotEmpty(startRowBytes)) {
            scan.setStartRow(startRowBytes);
        }

        scanHook.hook(scan);

        Filter filter = scan.getFilter();
        byte[] stopRowBytes = toBytes(stopRow);
        if (   ArrayUtils.isNotEmpty(stopRowBytes) 
            && !containsFilter(InclusiveStopFilter.class, filter)) {
            scan.setStopRow(stopRowBytes);
        }

        if (pageSize > 0) {
            if (filter == null) {
                filter = new PageFilter(pageSize);
            } else {
                if (!(filter instanceof FilterList)) {
                    filter = new FilterList(Operator.MUST_PASS_ALL, filter);
                }
                ((FilterList) filter).addFilter(new PageFilter(pageSize));
            }
        }
        scan.setFilter(filter);
        return scan;
    }

    protected ScanHook pageScanHook(PageQueryBuilder query) {
        return scan -> {
            FilterList filters = query.filters();
            byte[] stopRowBytes = toBytes(query.stopRowKey());
            if (ArrayUtils.isNotEmpty(stopRowBytes) && query.inclusiveStopRow()) {
                filters.addFilter(new InclusiveStopFilter(stopRowBytes));
            }

            // query column
            if (!containsFilter(FirstKeyOnlyFilter.class, filters)) {
                if (MapUtils.isNotEmpty(query.famQuaes())) {
                    query.famQuaes().forEach((key, qualifies) -> {
                        byte[] family = toBytes(key);
                        if (ArrayUtils.isEmpty(qualifies)) {
                            scan.addFamily(family);
                        } else {
                            Stream.of(qualifies).forEach(q -> scan.addColumn(family, toBytes(q)));
                        }
                    });
                } else {
                    addDefinedFamilies(scan);
                }
            }

            scan.setFilter(filters);
            //scan.setCacheBlocks(false);
            //scan.setCaching(0);
            //scan.setMaxResultSize(maxResultSize);
        };
    }

    // ------------------------------------------------------------------private methods
    private String toLowerCamel(String name) {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
    }

    private byte[] defaultIfEmpty(byte[] array, byte[] defaultArray) {
        return ArrayUtils.isEmpty(array) ? defaultArray : array;
    }

    private Object nearRowKey(String rowKeyPrefix, Object startRowKey, boolean isNext) {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(1, SortOrder.ASC);
        query.prefixRowKey(toBytes(rowKeyPrefix));
        query.startRowKey(startRowKey);
        List<T> result = isNext ? nextPage(query) : previousPage(query);
        return CollectionUtils.isEmpty(result)
               ? startRowKey : result.get(0).getRowKeyAsString();
    }

    private List<T> find(Object startRow, Object stopRow, int pageSize, boolean reversed, 
                         ScanHook scanHook, boolean inclusiveStartRow) {
        Scan scan = buildScan(startRow, stopRow, pageSize, reversed, scanHook);
        List<T> result = template.find(tableName, scan, rowMapper);

        // sort result
        Comparator<? super R> c = reversed ? Comparator.reverseOrder() : Comparator.naturalOrder();
        result.sort(Comparator.comparing(HbaseBean::getRowKey, Comparator.nullsLast(c)));
        //result.sort(Comparator.comparing(Function.identity(), Comparator.nullsLast(c)));

        if (   CollectionUtils.isNotEmpty(result) 
            && !inclusiveStartRow 
            && Arrays.equals(toBytes(startRow), result.get(0).getRowKeyAsBytes())
        ) {
            result = result.subList(1, result.size()); // the first is start row
        }
        return result;
    }

    private List<T> page(PageQueryBuilder query, boolean isNextPage, boolean reversed) {
        List<T> result = find(query.startRowKey(), query.stopRowKey(), 
                              query.actualPageSize(), reversed, 
                              pageScanHook(query), query.inclusiveStartRow());
        if (CollectionUtils.isNotEmpty(result)) {
            if (result.size() > query.pageSize()) {
                // the data from multiple region server 
                result = result.subList(0, query.pageSize());
            }

            if (!isNextPage) {
                Collections.reverse(result); // previous page
            }

            if (query.requireRowNum()) {
                for (int i = 0, n = result.size(); i < n; i++) {
                    result.get(i).setRowNum(i);
                }
            }
        }
        return result;
    }

    private void addDefinedFamilies(Scan scan) {
        this.definedFamiliesBytes.forEach(scan::addFamily);
    }

    private byte[] serialRowKey(@Nonnull R rowKey) {
        Objects.nonNull(rowKey);

        if (this.rowkeySerializer != null) {
            return this.rowkeySerializer.serialize(rowKey);
        } else {
            return toBytes(rowKey);
        }
    }

    private R deserialRowKey(@Nonnull byte[] rowKey) {
        Preconditions.checkArgument(ArrayUtils.isNotEmpty(rowKey));

        if (this.rowkeySerializer != null) {
            return this.rowkeySerializer.deserialize(rowKey, this.rowKeyType);
        } else {
            return fromBytes(rowKey, this.rowKeyType);
        }
    }

    // Only for HbaseEntity
    private static byte[] serialValue(Object target, Column column) {
        Object value = Fields.get(target, column.getField());
        if (value == null) {
            return null;
        } else if (column.getDateBytesConvert() != null) {
            return column.getDateBytesConvert().toBytes((Date) value);
        } else if (column.getSerializer() != null) {
            return column.getSerializer().serialize(value);
        } else {
            return toBytes(value); // others conditions
        }
    }

    // Only for HbaseEntity
    private Object deserialValue(Column column, byte[] bytes) {
        if (bytes == null || column == null) {
            return null; // null value or not exists qualifier field name
        }

        Class<?> fieldType = column.getField().getType();
        if (column.getDateBytesConvert() != null) {
            return column.getDateBytesConvert().toDate(bytes);
        } else if (column.getSerializer() != null) {
            return column.getSerializer().deserialize(bytes, fieldType);
        } else {
            return fromBytes(bytes, fieldType);
        }
    }

    private static <E extends Filter> boolean containsFilter(
                              Class<E> type, Filter filter) {
        if (filter == null) {
            return false;
        } else if (!(filter instanceof FilterList)) {
            return type == filter.getClass();
        } else {
            return ((FilterList) filter).getFilters().stream()
                                        .anyMatch(f -> type == f.getClass());
        }
    }

    private static String buildTableName(String namespace, String tableName) {
        return isBlank(namespace) 
             ? tableName.trim()
             : namespace.trim() + ":" + tableName.trim(); 
    }

    private Put buildPut(byte[] rowKey, byte[] family, long ts, Map<String, Object> data) {
        Put put = new Put(rowKey);
        for (Entry<String, Object> e : data.entrySet()) {
            String name; Object value;
            if (   isEmpty(name = e.getKey())
                || ROW_KEY_NAME.equals(name)
                || ROW_NUM_NAME.equals(name)
                || (value = e.getValue()) == null
            ) {
                continue;
            }
            // HbaseMap only support string value
            put.addColumn(family, toBytes(name), ts, toBytes(value.toString()));
        }
        return put;
    }

    /**
     * Hbase scan hook
     */
    @FunctionalInterface
    private interface ScanHook {
        void hook(Scan scan);
    }

    public static Serializer getSerializer(Class<? extends Serializer> clazz) {
        if (clazz == null || clazz == NullSerializer.class) {
            return null;
        }
        Serializer serizlizer = REGISTERED_SERIALIZER.get(clazz);
        if (serizlizer == null) {
            synchronized (REGISTERED_SERIALIZER) {
                if ((serizlizer = REGISTERED_SERIALIZER.get(clazz)) == null) {
                    try {
                        serizlizer = clazz.newInstance();
                    } catch (Exception e) {
                        serizlizer = NullSerializer.SINGLETON;
                    }
                    REGISTERED_SERIALIZER.put(clazz, serizlizer);
                }
            }
        }

        if (serizlizer == NullSerializer.SINGLETON) {
            throw new RuntimeException("Cannot create instance: " + clazz.getName());
        } else {
            return serizlizer;
        }
    }

}
