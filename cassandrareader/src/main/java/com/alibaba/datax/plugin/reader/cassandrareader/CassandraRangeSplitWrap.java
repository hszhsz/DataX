package com.alibaba.datax.plugin.reader.cassandrareader;

import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsRangeSplitWrap;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


/**
 * Desc:
 * Mail: shunshun.yss@alibaba-inc.com
 * Created by yueshunshun
 * Date: 2018/8/17
 */
public class CassandraRangeSplitWrap {

    public static List<String> splitAndWrap(String left, String right, int expectSliceNumber,
                                            String columnName, String quote, DataBaseType dataBaseType) {
        String[] tempResult = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);
        return wrapRange(tempResult, columnName, quote, dataBaseType);
    }

    public static List<String> splitAndWrap(BigInteger left, BigInteger right, int expectSliceNumber, String columnName) {
        BigInteger[] tempResult = RangeSplitUtil.doBigIntegerSplit(left, right, expectSliceNumber);
        return wrapRange(tempResult, columnName);
    }

    private static List<String> wrapRange(BigInteger[] rangeResult, String columnName) {
        String[] rangeStr = new String[rangeResult.length];
        for (int i = 0, len = rangeResult.length; i < len; i++) {
            rangeStr[i] = rangeResult[i].toString();
        }
        return wrapRange(rangeStr, columnName, "", null);
    }

    public static List<String> wrapRange(String[] rangeResult, String columnName,
                                         String quote, DataBaseType dataBaseType) {
        if (null == rangeResult || rangeResult.length < 2) {
            throw new IllegalArgumentException(String.format(
                    "Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[%s].",
                    StringUtils.join(rangeResult, ",")));
        }

        List<String> result = new ArrayList<String>();

        //TODO  change to  stringbuilder.append(..)
        if (2 == rangeResult.length) {
            result.add(String.format(" %s >= %s%s%s  AND %s <= %s%s%s ", columnName, quote, RdbmsRangeSplitWrap.quoteConstantValue(rangeResult[0], dataBaseType),
                    quote, columnName, quote, RdbmsRangeSplitWrap.quoteConstantValue(rangeResult[1], dataBaseType), quote));
            return result;
        } else {
            for (int i = 0, len = rangeResult.length - 2; i < len; i++) {
                result.add(String.format(" %s >= %s%s%s AND %s < %s%s%s ", columnName, quote, RdbmsRangeSplitWrap.quoteConstantValue(rangeResult[i], dataBaseType),
                        quote, columnName, quote, RdbmsRangeSplitWrap.quoteConstantValue(rangeResult[i + 1], dataBaseType), quote));
            }

            result.add(String.format(" %s >= %s%s%s AND %s <= %s%s%s ", columnName, quote, RdbmsRangeSplitWrap.quoteConstantValue(rangeResult[rangeResult.length - 2], dataBaseType),
                    quote, columnName, quote, RdbmsRangeSplitWrap.quoteConstantValue(rangeResult[rangeResult.length - 1], dataBaseType), quote));
            return result;
        }
    }
}
