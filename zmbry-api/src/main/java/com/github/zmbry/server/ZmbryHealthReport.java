package com.github.zmbry.server;

import java.util.Map;

/**
 * @author zifeng
 *
 */
public interface ZmbryHealthReport {

    String getQuotaStateFieldName();

    String getReportName();

    public Map<String, String> getRecentHealthReport();

    long getAggregateIntervalInMinutes();
}
