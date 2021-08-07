package org.codelibs.fesen.extension.analysis;

import static org.codelibs.fesen.runner.FesenRunner.newConfigs;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.Settings.Builder;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.node.Node;
import org.codelibs.fesen.runner.FesenRunner;
import org.codelibs.fesen.runner.net.EcrCurl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AlphaNumWordFilterFactoryTest {

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
    public void test_basic() throws Exception {

        runner.ensureYellow();
        Node node = runner.node();

        final String index = "dataset";

        final String indexSettings = "{\"index\":{\"analysis\":{" //
                + "\"filter\":{" //
                + "\"alphanum_word_filter\":{\"type\":\"alphanum_word\"}" + "}," //
                + "\"tokenizer\":{" //
                + "\"unigram_analyzer\":{\"type\":\"nGram\",\"min_gram\":\"1\",\"max_gram\":\"1\",\"token_chars\":[\"letter\",\"digit\"]}"
                + "},"//
                + "\"analyzer\":{" //
                + "\"ngram_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"unigram_analyzer\"},"
                + "\"ngram_word_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"unigram_analyzer\",\"filter\":[\"alphanum_word_filter\"]}"
                + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        {
            String text = "aaa";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(1, tokens.size());
                assertEquals("aaa", tokens.get(0).get("token").toString());
            }
        }

        {
            String text = "aaa bbb";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(2, tokens.size());
                assertEquals("aaa", tokens.get(0).get("token").toString());
                assertEquals("bbb", tokens.get(1).get("token").toString());
            }
        }

        {
            String text = "aa1 bb2 333";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(3, tokens.size());
                assertEquals("aa1", tokens.get(0).get("token").toString());
                assertEquals("bb2", tokens.get(1).get("token").toString());
                assertEquals("333", tokens.get(2).get("token").toString());
            }
        }

        {
            String text = "aaa亜aaa";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(3, tokens.size());
                assertEquals("aaa", tokens.get(0).get("token").toString());
                assertEquals("亜", tokens.get(1).get("token").toString());
                assertEquals("aaa", tokens.get(2).get("token").toString());
            }
        }

        {
            String text = "嬉しい";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(3, tokens.size());
                assertEquals("嬉", tokens.get(0).get("token").toString());
                assertEquals("し", tokens.get(1).get("token").toString());
                assertEquals("い", tokens.get(2).get("token").toString());
            }
        }

    }

    @Test
    public void test_maxTokenLength() throws Exception {

        runner.ensureYellow();
        Node node = runner.node();

        final String index = "dataset";

        final String indexSettings = "{\"index\":{\"analysis\":{" //
                + "\"filter\":{" //
                + "\"alphanum_word_filter\":{\"type\":\"alphanum_word\",\"max_token_length\":2}" + "}," //
                + "\"tokenizer\":{" //
                + "\"unigram_analyzer\":{\"type\":\"nGram\",\"min_gram\":\"1\",\"max_gram\":\"1\",\"token_chars\":[\"letter\",\"digit\"]}"
                + "},"//
                + "\"analyzer\":{" //
                + "\"ngram_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"unigram_analyzer\"},"
                + "\"ngram_word_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"unigram_analyzer\",\"filter\":[\"alphanum_word_filter\"]}"
                + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        {
            String text = "aaa";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(1, tokens.size());
                assertEquals("aa", tokens.get(0).get("token").toString());
            }
        }

        {
            String text = "aaa bbb";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(2, tokens.size());
                assertEquals("aa", tokens.get(0).get("token").toString());
                assertEquals("bb", tokens.get(1).get("token").toString());
            }
        }

        {
            String text = "aa1 bb2 333";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(3, tokens.size());
                assertEquals("aa", tokens.get(0).get("token").toString());
                assertEquals("bb", tokens.get(1).get("token").toString());
                assertEquals("33", tokens.get(2).get("token").toString());
            }
        }

        {
            String text = "aaa亜aaa";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(3, tokens.size());
                assertEquals("aa", tokens.get(0).get("token").toString());
                assertEquals("亜", tokens.get(1).get("token").toString());
                assertEquals("aa", tokens.get(2).get("token").toString());
            }
        }

        {
            String text = "嬉しい";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ngram_word_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(3, tokens.size());
                assertEquals("嬉", tokens.get(0).get("token").toString());
                assertEquals("し", tokens.get(1).get("token").toString());
                assertEquals("い", tokens.get(2).get("token").toString());
            }
        }

    }

}
