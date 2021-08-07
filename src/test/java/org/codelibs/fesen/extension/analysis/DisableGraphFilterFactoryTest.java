package org.codelibs.fesen.extension.analysis;

import static org.codelibs.fesen.runner.FesenRunner.newConfigs;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.action.support.WriteRequest.RefreshPolicy;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.Settings.Builder;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.index.query.QueryBuilders;
import org.codelibs.fesen.node.Node;
import org.codelibs.fesen.runner.FesenRunner;
import org.codelibs.fesen.runner.net.EcrCurl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DisableGraphFilterFactoryTest {

    private FesenRunner runner;

    private int numOfNode = 1;

    private String clusterName;

    @Before
    public void setUp() throws Exception {
        clusterName = "es-analysisja-" + System.currentTimeMillis();
        runner = new FesenRunner();
        runner.onBuild(new FesenRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.put("discovery.type", "single-node");
                // settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
                // settingsBuilder.putList("cluster.initial_master_nodes", "127.0.0.1:9301");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(numOfNode).pluginTypes("org.codelibs.fesen.extension.ExtensionPlugin"));

    }

    @After
    public void cleanUp() throws Exception {
        runner.close();
        runner.clean();
    }

    @Test
    public void test_disableGraph() throws Exception {
        runner.ensureYellow();
        Node node = runner.node();

        final String index = "dataset";

        final String indexSettings = "{\"index\":{\"analysis\":{"//
                + "\"analyzer\":{"
                + "\"ja_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"reloadable_kuromoji_tokenizer\",\"filter\":[\"disable_graph\"]}"
                + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();
        runner.createMapping(index, "data",
                "{\"data\":{\"properties\":{\"content\" : {\"type\" : \"text\",\"analyzer\":\"ja_analyzer\"}}}}");
        try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                .body("{\"analyzer\":\"ja_analyzer\",\"text\":\"レッドハウスフーズ\"}").execute()) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
            assertEquals(3, tokens.size());
            assertEquals("レッド", tokens.get(0).get("token").toString());
            assertEquals("レッドハウスフーズ", tokens.get(1).get("token").toString());
            assertEquals("ハウスフーズ", tokens.get(2).get("token").toString());
        }

        runner.insert(index, "data", "1",
                builder -> builder.setSource("{\"content\":\"レッド\"}", XContentType.JSON).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL));

        SearchResponse response = runner.search(index, builder -> builder.setQuery(QueryBuilders.matchQuery("content", "レッドハウスフーズ")));
        assertEquals(1L, response.getHits().getTotalHits().value);
    }
}
