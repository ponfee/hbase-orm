package cn.ponfee.hbase.model;

import cn.ponfee.commons.collect.Collects;
import cn.ponfee.commons.model.SortOrder;
import cn.ponfee.commons.util.ObjectUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.springframework.util.Assert;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static cn.ponfee.commons.util.Bytes.toBytes;

/**
 * Page query for hbase
 * 
 * hbase filter: https://blog.csdn.net/cnweike/article/details/42920547
 * 
 * @author Ponfee
 */
public class PageQueryBuilder {

    private final int pageSize;
    private final SortOrder sortOrder;

    private Object startRowKey;
    private Boolean inclusiveStartRow;
    private Object stopRowKey;
    private Boolean inclusiveStopRow;

    private Map<String, String[]> famQuaes; // query the spec family and qualifier
    private final FilterList filters = new FilterList(Operator.MUST_PASS_ALL);

    private boolean requireRowNum = true; // query result whether include row number

    private PageQueryBuilder(int pageSize, SortOrder sortOrder) {
        Preconditions.checkArgument(
            pageSize > 0, "PageSize[" + pageSize + "] must be greater than 0."
        );
        Preconditions.checkArgument(
            sortOrder != null, "SortOrder cannot be null."
        );
        this.pageSize = pageSize;
        this.sortOrder = sortOrder;
    }

    public static PageQueryBuilder newBuilder(int pageSize) {
        return new PageQueryBuilder(pageSize, SortOrder.ASC);
    }

    public static PageQueryBuilder newBuilder(
        int pageSize, SortOrder sortOrder) {
        return new PageQueryBuilder(pageSize, sortOrder);
    }

    public void startRowKey(Object startRowKey) {
        this.startRowKey(startRowKey, null);
    }

    public void startRowKey(Object startRowKey, Boolean inclusiveStartRow) {
        this.startRowKey = startRowKey;
        this.inclusiveStartRow = inclusiveStartRow;
    }

    public void stopRowKey(Object stopRowKey) {
        this.stopRowKey(stopRowKey, null);
    }

    public void stopRowKey(Object stopRowKey, Boolean inclusiveStopRow) {
        this.stopRowKey = stopRowKey;
        this.inclusiveStopRow = inclusiveStopRow;
    }

    public void requireRowNum(boolean requireRowNum) {
        this.requireRowNum = requireRowNum;
    }

    public void addColumns(@Nonnull String family, String... quaes) {
        Assert.notNull(family, "Column family cannot be null.");
        if (famQuaes == null) {
            famQuaes = new HashMap<>();
        }
        String[] quaes0 = famQuaes.get(family);
        if (ArrayUtils.isEmpty(quaes0)) {
            famQuaes.put(family, quaes);
        } else if (ArrayUtils.isNotEmpty(quaes)) {
            famQuaes.put(family, Collects.concat(String[]::new, quaes0, quaes));
        }
    }

    // ----------------------------------------------------------------query conditions
    public PageQueryBuilder equals(String family, String qualifier, byte[] value) {
        this.filters.addFilter(equals(family, qualifier, value, true));
        return this;
    }

    public PageQueryBuilder notEquals(String family, String qualifier, byte[] value) {
        this.filters.addFilter(equals(family, qualifier, value, false));
        return this;
    }

    public <T> PageQueryBuilder in(String family, String qualifier, byte[]... values) {
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (byte[] value : values) {
            filters.addFilter(equals(family, qualifier, value, true));
        }
        this.filters.addFilter(filters);
        return this;
    }

    public PageQueryBuilder notIn(String family, String qualifier, byte[]... values) {
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        for (byte[] value : values) {
            filters.addFilter(equals(family, qualifier, value, false));
        }
        this.filters.addFilter(filters);
        return this;
    }

    public PageQueryBuilder exists(String family, String qualifier) {
        // must not be empty value
        SingleColumnValueFilter filter = equals(family, qualifier, ArrayUtils.EMPTY_BYTE_ARRAY, false);
        filter.setFilterIfMissing(true); // 若该列不存在，则过滤掉
        this.filters.addFilter(filter);
        return this;
    }

    public PageQueryBuilder notExists(String family, String qualifier) {
        // must be empty value
        SingleColumnValueFilter filter = equals(family, qualifier, ArrayUtils.EMPTY_BYTE_ARRAY, true);
        filter.setFilterIfMissing(false); // 若该列不存在也会包含在结果集中
        this.filters.addFilter(filter);
        return this;
    }

    public PageQueryBuilder greater(String family, String qualifier,
                                    byte[] min, boolean inclusive) {
        this.filters.addFilter(greater0(family, qualifier, min, inclusive));
        return this;
    }

    public PageQueryBuilder less(String family, String qualifier,
                                 byte[] max, boolean inclusive) {
        this.filters.addFilter(less0(family, qualifier, max, inclusive));
        return this;
    }

    /*public PageQueryBuilder range(String family, String qualifier,
                                  byte[] min, boolean inclusiveMin,
                                  byte[] max, boolean inclusiveMax) {
        this.filters.addFilter(greater0(family, qualifier, min, inclusiveMin)); // >(=) min
        this.filters.addFilter(less0(family, qualifier, max, inclusiveMax));    // <(=) max
        return this;
    }

    public PageQueryBuilder notRange(String family, String qualifier,
                                     byte[] min, boolean inclusiveMin,
                                     byte[] max, boolean inclusiveMax) {
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filters.addFilter(less0(family, qualifier, min, !inclusiveMin));    // <(=) min
        filters.addFilter(greater0(family, qualifier, max, !inclusiveMax)); // >(=) max
        this.filters.addFilter(filters);
        return this;
    }*/

    // include min, exclude max
    public PageQueryBuilder range(String family, String qualifier, 
                                  byte[] min, byte[] max) {
        this.filters.addFilter(greater0(family, qualifier, min, true)); // >= min  (include)
        this.filters.addFilter(less0(family, qualifier, max, false));   // <  max  (exclude)
        return this;
    }

    // exclude min, include max
    public PageQueryBuilder notRange(String family, String qualifier,
                                     byte[] min, byte[] max) {
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filters.addFilter(less0(family, qualifier, min, false));   // <  min  (exclude)
        filters.addFilter(greater0(family, qualifier, max, true)); // >= max  (include)
        this.filters.addFilter(filters);
        return this;
    }

    public PageQueryBuilder regexp(String family, String qualifier, String regexp) {
        this.filters.addFilter(regexp(family, qualifier, regexp, true));
        return this;
    }

    public PageQueryBuilder notRegexp(String family, String qualifier, String regexp) {
        this.filters.addFilter(regexp(family, qualifier, regexp, false));
        return this;
    }

    public PageQueryBuilder prefix(String family, String qualifier, byte[] prefix) {
        this.filters.addFilter(prefix(family, qualifier, prefix, true));
        return this;
    }

    public PageQueryBuilder notPrefix(String family, String qualifier, byte[] prefix) {
        this.filters.addFilter(prefix(family, qualifier, prefix, false));
        return this;
    }

    public PageQueryBuilder like(String family, String qualifier, String wildcard) {
        this.filters.addFilter(like(family, qualifier, wildcard, true));
        return this;
    }

    public PageQueryBuilder notLike(String family, String qualifier, String wildcard) {
        this.filters.addFilter(like(family, qualifier, wildcard, false));
        return this;
    }

    // ------------------------------------------------------------row key filter
    public PageQueryBuilder rowKeyOnly() {
        // origin data: {"name":"pUqnw","rowKey":"20181004162958","age":"3"}

        this.filters.addFilter(new FirstKeyOnlyFilter()); // {"rowKey":"20181004162958","age":"3"}

        //this.filters.addFilter(new KeyOnlyFilter()); // {"name":"","rowKey":"20181004162958","age":""}

        return this;
    }

    public PageQueryBuilder likeRowKey(String keyword) {
        this.filters.addFilter(likeKey(keyword, true));
        return this;
    }

    public PageQueryBuilder notLikeRowKey(String keyword) {
        this.filters.addFilter(likeKey(keyword, false));
        return this;
    }

    public PageQueryBuilder regexpRowKey(String rowKeyRegexp) {
        this.filters.addFilter(regexpKey(rowKeyRegexp, true));
        return this;
    }

    public PageQueryBuilder notRegexpRowKey(String rowKeyRegexp) {
        this.filters.addFilter(regexpKey(rowKeyRegexp, false));
        return this;
    }

    public PageQueryBuilder prefixRowKey(byte[] prefixKey) {
        this.filters.addFilter(prefixKey(prefixKey, true));
        return this;
    }

    public PageQueryBuilder notPrefixRowKey(byte[] prefixKey) {
        this.filters.addFilter(prefixKey(prefixKey, false));
        return this;
    }

    public PageQueryBuilder equalsRowKey(byte[] rowKey) {
        this.filters.addFilter(equalsKey(rowKey, true));
        return this;
    }

    public PageQueryBuilder notEqualsRowKey(byte[] rowKey) {
        this.filters.addFilter(equalsKey(rowKey, false));
        return this;
    }

    // ---------------------------------------------------------custom filter
    public PageQueryBuilder customFilter(Filter filter) {
        this.filters.addFilter(filter);
        return this;
    }

    // ---------------------------------------------------------getter
    public int pageSize() {
        return pageSize;
    }

    public int actualPageSize() {
        return inclusiveStartRow() ? pageSize : pageSize + 1;
    }

    public Object startRowKey() {
        return startRowKey;
    }

    public Object stopRowKey() {
        return stopRowKey;
    }

    public Map<String, String[]> famQuaes() {
        return famQuaes;
    }

    public boolean requireRowNum() {
        return requireRowNum;
    }

    public SortOrder sortOrder() {
        return Optional.ofNullable(sortOrder).orElse(SortOrder.ASC);
    }

    public boolean inclusiveStartRow() {
        return Optional.ofNullable(inclusiveStartRow)
                       .orElse(ObjectUtils.isEmpty(startRowKey));
    }

    public Boolean inclusiveStopRow() {
        return Optional.ofNullable(inclusiveStopRow).orElse(true);
    }

    public FilterList filters() {
        return filters;
    }

    // ----------------------------------------------------------------page start
    public <T> T nextPageStartRow(List<T> results) {
        return pageStartRow(results, true);
    }

    public <T> T previousPageStartRow(List<T> results) {
        return pageStartRow(results, false);
    }

    // ------------------------------------------------------------private methods
    private <T> T pageStartRow(List<T> results, boolean isNextPage) {
        if (CollectionUtils.isEmpty(results)) {
            return null;
        }

        return results.get(isNextPage ? results.size() - 1 : 0);
    }

    // ------------------------------------------------------------private static methods
    private static SingleColumnValueFilter equals(String family, String qualifier,
                                                  byte[] value, boolean predicate) {
        return new SingleColumnValueFilter(
            toBytes(family), toBytes(qualifier), 
            eq(predicate), value
        );
      //new SingleColumnValueExcludeFilter(toBytes(family), toBytes(qualifier), CompareOp.EQUAL, value);
    }

    // >(=) min
    private static Filter greater0(String family, String qualifier,
                                   byte[] min, boolean inclusive) {
        return new SingleColumnValueFilter(
            toBytes(family), toBytes(qualifier), 
            inclusive ? CompareOp.GREATER_OR_EQUAL : CompareOp.GREATER, 
            min
        );
    }

    // <(=) max
    private static Filter less0(String family, String qualifier,
                                byte[] max, boolean inclusive) {
        return new SingleColumnValueFilter(
            toBytes(family), toBytes(qualifier), 
            inclusive ? CompareOp.LESS_OR_EQUAL : CompareOp.LESS, 
            max
        );
    }

    private static Filter like(String family, String qualifier, 
                               String wildcard, boolean predicate) {
        return new SingleColumnValueFilter(
            toBytes(family), toBytes(qualifier), 
            eq(predicate), new SubstringComparator(wildcard)
        );
    }

    private static Filter prefix(String family, String qualifier, 
                                 byte[] prefix, boolean predicate) {
        return new SingleColumnValueFilter(
            toBytes(family), toBytes(qualifier),
            eq(predicate), new BinaryPrefixComparator(prefix)
        );
    }

    private static Filter regexp(String family, String qualifier, 
                                 String regexp, boolean predicate) {
        return new SingleColumnValueFilter(
            toBytes(family), toBytes(qualifier), 
            eq(predicate), new RegexStringComparator(regexp)
        );
    }

    private static Filter regexpKey(String rowKeyRegexp, boolean predicate) {
        return new RowFilter(eq(predicate), new RegexStringComparator(rowKeyRegexp));
    }

    private static Filter likeKey(String keyword, boolean predicate) {
        return new RowFilter(eq(predicate), new SubstringComparator(keyword));
    }

    private static Filter prefixKey(byte[] keyPrefix, boolean predicate) {
        //return new PrefixFilter(keyPrefix);
        return new RowFilter(eq(predicate), new BinaryPrefixComparator(keyPrefix));
    }

    private static Filter equalsKey(byte[] rowKey, boolean predicate) {
        return new RowFilter(eq(predicate), new BinaryComparator(rowKey));
    }

    private static CompareOp eq(boolean predicate) {
        return predicate ? CompareOp.EQUAL : CompareOp.NOT_EQUAL;
    }
}
