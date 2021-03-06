package org.codelibs.fesen.extension;

import static org.codelibs.fesen.runner.FesenRunner.newConfigs;
import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.action.index.IndexResponse;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.index.query.QueryBuilders;
import org.codelibs.fesen.node.Node;
import org.codelibs.fesen.rest.RestStatus;
import org.codelibs.fesen.runner.FesenRunner;
import org.codelibs.fesen.runner.net.EcrCurl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExtensionPluginTest {

    private FesenRunner runner;

    private File[] userDictFiles;

    private final int numOfNode = 1;

    private final int numOfDocs = 1000;

    private String clusterName;

    @Before
    public void setUp() throws Exception {
        clusterName = "es-analysisja-" + System.currentTimeMillis();
        runner = new FesenRunner();
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
            settingsBuilder.put("http.cors.allow-origin", "*");
            settingsBuilder.put("discovery.type", "single-node");
            // settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
            // settingsBuilder.putList("cluster.initial_master_nodes", "127.0.0.1:9301");
        }).build(newConfigs().clusterName(clusterName).numOfNode(numOfNode).pluginTypes("org.codelibs.fesen.extension.ExtensionPlugin,"
                + "org.codelibs.fesen.extension.kuromoji.plugin.analysis.kuromoji.AnalysisKuromojiPlugin"));

        userDictFiles = null;
    }

    private void updateDictionary(final File file, final String content)
            throws IOException, UnsupportedEncodingException, FileNotFoundException {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
            bw.write(content);
            bw.flush();
        }
    }

    @After
    public void cleanUp() throws Exception {
        runner.close();
        runner.clean();
        if (userDictFiles != null) {
            for (final File file : userDictFiles) {
                file.deleteOnExit();
            }
        }
    }

    @Test
    public void test_kuromoji() throws Exception {
        userDictFiles = new File[numOfNode];
        for (int i = 0; i < numOfNode; i++) {
            final String homePath = runner.getNode(i).settings().get("path.home");
            userDictFiles[i] = new File(new File(homePath, "config"), "userdict_ja.txt");
            updateDictionary(userDictFiles[i], "????????????????????????,?????? ??????????????????,??????????????? ??????????????????,??????????????????");
        }

        runner.ensureYellow();
        final Node node = runner.node();

        final String index = "dataset";
        final String type = "item";

        final String indexSettings = "{\"index\":{\"analysis\":{" + "\"tokenizer\":{"//
                + "\"kuromoji_user_dict\":{\"type\":\"kuromoji_tokenizer\",\"mode\":\"extended\",\"user_dictionary\":\"userdict_ja.txt\"},"
                + "\"kuromoji_user_dict_reload\":{\"type\":\"reloadable_kuromoji\",\"mode\":\"extended\",\"user_dictionary\":\"userdict_ja.txt\",\"reload_interval\":\"1s\"}"
                + "},"//
                + "\"analyzer\":{"
                + "\"ja_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"kuromoji_user_dict\",\"filter\":[\"reloadable_kuromoji_stemmer\"]},"
                + "\"ja_reload_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"kuromoji_user_dict_reload\",\"filter\":[\"reloadable_kuromoji_stemmer\"]}"
                + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "keyword")//
                .endObject()//

                // msg1
                .startObject("msg1")//
                .field("type", "text")//
                .field("analyzer", "ja_reload_analyzer")//
                .endObject()//

                // msg2
                .startObject("msg2")//
                .field("type", "text")//
                .field("analyzer", "ja_analyzer")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        final IndexResponse indexResponse1 =
                runner.insert(index, type, "1", "{\"msg1\":\"????????????????????????\", \"msg2\":\"????????????????????????\", \"id\":\"1\"}");
        assertEquals(RestStatus.CREATED, indexResponse1.status());
        runner.refresh();

        String text;
        for (int i = 0; i < 1000; i++) {
            text = "????????????????????????";
            assertDocCount(1, index, type, "msg1", text);
            assertDocCount(1, index, type, "msg2", text);

            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_reload_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals("??????", tokens.get(0).get("token").toString());
                assertEquals("???????????????", tokens.get(1).get("token").toString());
            }

            text = "?????????";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_reload_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals("???", tokens.get(0).get("token").toString());
                assertEquals("??????", tokens.get(1).get("token").toString());
            }
        }

        // changing a file timestamp
        Thread.sleep(2000);

        for (int i = 0; i < numOfNode; i++) {
            updateDictionary(userDictFiles[i], "????????????????????????,?????? ????????? ?????????,??????????????? ????????? ?????????,??????????????????\n" + "?????????,?????????,????????????????????????,??????");
        }

        final IndexResponse indexResponse2 =
                runner.insert(index, type, "2", "{\"msg1\":\"????????????????????????\", \"msg2\":\"????????????????????????\", \"id\":\"2\"}");
        assertEquals(RestStatus.CREATED, indexResponse2.status());
        runner.refresh();

        for (int i = 0; i < 1000; i++) {
            text = "????????????????????????";
            assertDocCount(1, index, type, "msg1", text);
            assertDocCount(2, index, type, "msg2", text);

            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_reload_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals("??????", tokens.get(0).get("token").toString());
                assertEquals("?????????", tokens.get(1).get("token").toString());
                assertEquals("?????????", tokens.get(2).get("token").toString());
            }

            text = "?????????";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_reload_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(text, tokens.get(0).get("token").toString());
            }
        }
    }

    @Test
    public void test_iteration_mark() throws Exception {
        runner.ensureYellow();
        final Node node = runner.node();

        final String index = "dataset";
        final String type = "item";

        final String indexSettings = "{\"index\":{\"analysis\":{" + "\"analyzer\":{"
                + "\"ja_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"},"
                + "\"ja_imark_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\",\"char_filter\":[\"iteration_mark\"]}" + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "keyword")//
                .endObject()//

                // msg1
                .startObject("msg1")//
                .field("type", "text")//
                .field("analyzer", "ja_imark_analyzer")//
                .endObject()//

                // msg2
                .startObject("msg2")//
                .field("type", "text")//
                .field("analyzer", "ja_analyzer")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        final IndexResponse indexResponse1 = runner.insert(index, type, "1", "{\"msg1\":\"??????\", \"msg2\":\"??????\", \"id\":\"1\"}");
        assertEquals(RestStatus.CREATED, indexResponse1.status());
        runner.refresh();

        final String[] inputs = { "?????? ??????", "????????? ?????????", "?????????????????? ??????????????????", "????????? ?????????", "?????? ??????", "???????????? ????????????", "?????? ??????" };

        assertDocCount(1, index, type, "msg1", "??????");
        assertDocCount(1, index, type, "msg1", "??????");
        assertDocCount(1, index, type, "msg2", "??????");

        for (final String input : inputs) {
            final String[] values = input.split(" ");
            final String text = values[0];
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_imark_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(values[1], tokens.get(0).get("token").toString());
            }
        }

    }

    @Test
    public void test_prolonged_sound_mark() throws Exception {
        runner.ensureYellow();
        final Node node = runner.node();

        final String index = "dataset";
        final String type = "item";

        final String indexSettings = "{\"index\":{\"analysis\":{" + "\"analyzer\":{"
                + "\"ja_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"},"
                + "\"ja_psmark_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\",\"char_filter\":[\"prolonged_sound_mark\"]}"
                + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "keyword")//
                .endObject()//

                // msg1
                .startObject("msg1")//
                .field("type", "text")//
                .field("analyzer", "ja_psmark_analyzer")//
                .endObject()//

                // msg2
                .startObject("msg2")//
                .field("type", "text")//
                .field("analyzer", "ja_analyzer")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        final IndexResponse indexResponse1 = runner.insert(index, type, "1", "{\"msg1\":\"??????\", \"msg2\":\"??????\", \"id\":\"1\"}");
        assertEquals(RestStatus.CREATED, indexResponse1.status());
        runner.refresh();

        final String[] psms =
                { "\u002d", "\uff0d", "\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u207b", "\u208b", "\u30fc" };

        assertDocCount(1, index, type, "msg1", "??????");
        assertDocCount(1, index, type, "msg2", "??????");

        for (final String psm : psms) {
            final String text = "???" + psm;
            assertDocCount(1, index, type, "msg1", text);
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_psmark_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals("??????", tokens.get(0).get("token").toString());
            }
        }

    }

    @Test
    public void test_kanji_number() throws Exception {
        runner.ensureYellow();
        final Node node = runner.node();

        final String index = "dataset";
        final String type = "item";

        final String indexSettings = "{\"index\":{\"analysis\":{" + "\"tokenizer\":{"//
                + "\"kuromoji_user_dict\":{\"type\":\"kuromoji_tokenizer\",\"mode\":\"extended\"}" + "},"//
                + "\"analyzer\":{" + "\"ja_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"kuromoji_user_dict\"},"
                + "\"ja_knum_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"kuromoji_user_dict\",\"filter\":[\"kanji_number\"]}" + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "keyword")//
                .endObject()//

                // msg1
                .startObject("msg1")//
                .field("type", "text")//
                .field("analyzer", "ja_knum_analyzer")//
                .endObject()//

                // msg2
                .startObject("msg2")//
                .field("type", "text")//
                .field("analyzer", "ja_analyzer")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        final IndexResponse indexResponse1 = runner.insert(index, type, "1", "{\"msg1\":\"????????????\", \"msg2\":\"????????????\", \"id\":\"1\"}");
        assertEquals(RestStatus.CREATED, indexResponse1.status());
        runner.refresh();

        assertDocCount(1, index, type, "msg1", "????????????");
        assertDocCount(1, index, type, "msg1", "12??????");
        assertDocCount(1, index, type, "msg2", "????????????");
        assertDocCount(0, index, type, "msg2", "12??????");

        final String text = "??????????????????";
        try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                .body("{\"analyzer\":\"ja_knum_analyzer\",\"text\":\"" + text + "\"}").execute()) {
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> tokens = (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
            assertEquals("190000000", tokens.get(0).get("token").toString());
            assertEquals("???", tokens.get(1).get("token").toString());
        }

    }

    private void assertDocCount(final int expected, final String index, final String type, final String field, final String value) {
        final SearchResponse searchResponse = runner.search(index, type, QueryBuilders.matchPhraseQuery(field, value), null, 0, numOfDocs);
        assertEquals(expected, searchResponse.getHits().getTotalHits().value);
    }
}
