package com.hxqh.custom;


import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Ocean lin on 2020/3/9.
 *
 * @author Ocean lin
 */
public class ElasticsearchInput extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {


    private final List<HttpHost> httpHosts;
    private final RestClientFactory restClientFactory;
    private final String index;

    private transient RestHighLevelClient client;

    private String scrollId;
    private SearchRequest searchRequest;

    private transient SearchSourceBuilder queryBuilder;

    private RowTypeInfo rowTypeInfo;

    private boolean hasNext;

    private Iterator<Map<String, Object>> iterator;
    private Map<String, Integer> position;


    public ElasticsearchInput(List<HttpHost> httpHosts,
                              RestClientFactory restClientFactory,
                              String index,
                              SearchSourceBuilder queryBuilder) {
        this.httpHosts = httpHosts;
        this.restClientFactory = restClientFactory;
        this.index = index;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        search();
    }

    /**
     * 从 es 中获取数据
     *
     * @throws IOException
     */
    protected void search() throws IOException {
        SearchResponse searchResponse;
        if (scrollId == null) {
            searchResponse = client.search(searchRequest);
            scrollId = searchResponse.getScrollId();
        } else {
            searchResponse = client.searchScroll(new SearchScrollRequest(scrollId));
        }

        if (searchResponse == null || searchResponse.getHits().getTotalHits() < 1) {
            hasNext = false;
            return;
        }

        hasNext = true;
        iterator = Arrays.stream(searchResponse.getHits().getHits())
                .map(t -> t.getSourceAsMap())
                .collect(Collectors.toList()).iterator();
    }


    @Override
    public void openInputFormat() throws IOException {
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
        restClientFactory.configureRestClientBuilder(builder);
        client = new RestHighLevelClient(builder);

        position = new CaseInsensitiveMap();
        int i = 0;
        for (String name : rowTypeInfo.getFieldNames()) {
            position.put(name, i++);
        }

        if (null == queryBuilder) {
            queryBuilder = new SearchSourceBuilder();
            queryBuilder.query(QueryBuilders.matchAllQuery());
        }

        // 批次大小设置
        queryBuilder.size(500);

        queryBuilder.fetchSource(rowTypeInfo.getFieldNames(), null);

        // 使用 scroll api 获取数据
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(3L));

        searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.scroll(scroll);
        searchRequest.source(queryBuilder);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        if (!hasNext) {
            return null;
        }

        if (!iterator.hasNext()) {
            this.search();
            if (!hasNext || !iterator.hasNext()) {
                hasNext = false;
                return null;
            }
        }

        for (Map.Entry<String, Object> entry : iterator.next().entrySet()) {
            Integer p = position.get(entry.getKey());
            if (p == null) {
                throw new IOException("unknown field " + entry.getKey());
            }

            reuse.setField(p, entry.getValue());
        }

        return reuse;
    }

    @Override
    public void close() throws IOException {
        if (client == null) {
            return;
        }


        iterator = null;
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest).isSucceeded();

        client.close();
        client = null;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowTypeInfo;
    }

    public static Builder builder(List<HttpHost> httpHosts, String index, SearchSourceBuilder builder) {
        return new Builder(httpHosts, index, builder);
    }

    @PublicEvolving
    public static class Builder {
        private final List<HttpHost> httpHosts;
        private String index;
        private RowTypeInfo rowTypeInfo;
        private SearchSourceBuilder queryBuilder;
        private RestClientFactory restClientFactory = restClientBuilder -> {
        };

        public Builder(List<HttpHost> httpHosts, String index, SearchSourceBuilder builder) {
            this.httpHosts = Preconditions.checkNotNull(httpHosts);
            this.index = index;
            this.queryBuilder = builder;
        }


        public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
            return this;
        }


        public ElasticsearchInput build() {
            Preconditions.checkNotNull(this.rowTypeInfo);
            ElasticsearchInput input = new ElasticsearchInput(httpHosts, restClientFactory, index, queryBuilder);
            input.rowTypeInfo = this.rowTypeInfo;
            input.queryBuilder = this.queryBuilder;
            return input;
        }

    }


}
