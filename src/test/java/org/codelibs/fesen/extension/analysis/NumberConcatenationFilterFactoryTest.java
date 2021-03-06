package org.codelibs.fesen.extension.analysis;

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
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.node.Node;
import org.codelibs.fesen.runner.FesenRunner;
import org.codelibs.fesen.runner.net.EcrCurl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NumberConcatenationFilterFactoryTest {

    private FesenRunner runner;

    private final int numOfNode = 1;

    private File[] numberSuffixFiles;

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
        }).build(newConfigs().clusterName(clusterName).numOfNode(numOfNode).pluginTypes("org.codelibs.fesen.extension.ExtensionPlugin"));

        numberSuffixFiles = null;
    }

    @After
    public void cleanUp() throws Exception {
        runner.close();
        runner.clean();
        if (numberSuffixFiles != null) {
            for (final File file : numberSuffixFiles) {
                file.deleteOnExit();
            }
        }
    }

    @Test
    public void test_basic() throws Exception {
        numberSuffixFiles = new File[numOfNode];
        for (int i = 0; i < numOfNode; i++) {
            final String homePath = runner.getNode(i).settings().get("path.home");
            numberSuffixFiles[i] = new File(new File(homePath, "config"), "number_suffix.txt");
            updateDictionary(numberSuffixFiles[i], "???\n???");
        }

        runner.ensureYellow();
        final Node node = runner.node();

        final String index = "dataset";

        final String indexSettings = "{\"index\":{\"analysis\":{" + "\"filter\":{"
                + "\"number_concat_filter\":{\"type\":\"number_concat\",\"suffix_words_path\":\"number_suffix.txt\"}" + "},"//
                + "\"analyzer\":{" + "\"ja_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"},"
                + "\"ja_concat_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\",\"filter\":[\"number_concat_filter\"]}" + "}"//
                + "}}}";
        runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
        runner.ensureYellow();

        {
            final String text = "100 ???";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_concat_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(1, tokens.size());
                assertEquals("100???", tokens.get(0).get("token").toString());
            }
        }

        {
            final String text = "aaa 100 ???";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_concat_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(2, tokens.size());
                assertEquals("aaa", tokens.get(0).get("token").toString());
                assertEquals("100???", tokens.get(1).get("token").toString());
            }
        }

        {
            final String text = "1 1 ??? 2 100 ??? 3";
            try (CurlResponse response = EcrCurl.post(node, "/" + index + "/_analyze").header("Content-Type", "application/json")
                    .body("{\"analyzer\":\"ja_concat_analyzer\",\"text\":\"" + text + "\"}").execute()) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> tokens =
                        (List<Map<String, Object>>) response.getContent(EcrCurl.jsonParser()).get("tokens");
                assertEquals(5, tokens.size());
                assertEquals("1", tokens.get(0).get("token").toString());
                assertEquals("1???", tokens.get(1).get("token").toString());
                assertEquals("2", tokens.get(2).get("token").toString());
                assertEquals("100???", tokens.get(3).get("token").toString());
                assertEquals("3", tokens.get(4).get("token").toString());
            }
        }
    }

    private void updateDictionary(final File file, final String content)
            throws IOException, UnsupportedEncodingException, FileNotFoundException {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
            bw.write(content);
            bw.flush();
        }
    }
}
