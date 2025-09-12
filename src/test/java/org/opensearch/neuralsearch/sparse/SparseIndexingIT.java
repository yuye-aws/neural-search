/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;

/**
 * Integration tests for sparse index feature
 */
public class SparseIndexingIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-index";
    private static final String NON_SPARSE_TEST_INDEX_NAME = TEST_INDEX_NAME + "_non_sparse";
    private static final String INVALID_PARAM_TEST_INDEX_NAME = TEST_INDEX_NAME + "_invalid";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final List<String> TEST_TOKENS = List.of("1000", "2000", "3000", "4000", "5000");
    private static final String PIPELINE_NAME = "seismic_test_pipeline";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Test creating an index with sparse index setting enabled
     */
    public void testCreateSparseIndex() throws IOException {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, 8);

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        assertEquals("true", indexSettingsMap.get("sparse"));
    }

    /**
     * Test indexing documents with sparse tokens field
     */
    public void testIndexDocumentsWithSparseTokensField() throws IOException {
        // Create index with sparse index setting enabled
        testCreateSparseIndex();

        // Create a document with sparse tokens field
        Map<String, Float> sparseTokens = createRandomTokenWeightMap(TEST_TOKENS);

        // Index the document
        addSparseEncodingDoc(TEST_INDEX_NAME, "1", List.of(TEST_SPARSE_FIELD_NAME), List.of(sparseTokens));

        // Verify document was indexed
        assertEquals(1, getDocCount(TEST_INDEX_NAME));

        // Get the document and verify its content
        Map<String, Object> document = getDocById(TEST_INDEX_NAME, "1");
        assertNotNull(document);

        Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertNotNull(source);

        Map<String, Object> sparseField = (Map<String, Object>) source.get(TEST_SPARSE_FIELD_NAME);
        assertNotNull(sparseField);

        // Verify the sparse tokens are present
        for (String token : TEST_TOKENS) {
            if (sparseTokens.containsKey(token)) {
                assertTrue(sparseField.containsKey(token));
                assertEquals(sparseTokens.get(token).doubleValue(), ((Number) sparseField.get(token)).doubleValue(), 0.001);
            }
        }
    }

    /**
     * Test creating an index with sparse index setting disabled (default)
     */
    public void testCreateNonSparseIndex() throws IOException {
        // Create index without sparse index setting (default is false)
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
        String indexMappings = prepareIndexMapping(100, 0.4f, 0.1f, 8, TEST_SPARSE_FIELD_NAME);

        Request request = new Request("PUT", "/" + NON_SPARSE_TEST_INDEX_NAME);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(NON_SPARSE_TEST_INDEX_NAME));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + NON_SPARSE_TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(NON_SPARSE_TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        // Sparse setting should not be present (default is false)
        assertFalse(indexSettingsMap.containsKey("sparse"));
    }

    /**
     * Test that sparse index setting cannot be updated after index creation (it's a final setting)
     */
    public void testCannotUpdateSparseIndexSetting() throws IOException {
        // Create index without sparse index setting (default is false)
        testCreateNonSparseIndex();

        // Try to update the sparse index setting (should fail because it's final)
        Request updateSettingsRequest = new Request("PUT", "/" + NON_SPARSE_TEST_INDEX_NAME + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" + "  \"index\": {\n" + "    \"sparse\": true\n" + "  }\n" + "}");

        // This should throw an exception because sparse is a final setting
        expectThrows(IOException.class, () -> { client().performRequest(updateSettingsRequest); });
    }

    /**
     * Test error handling when creating a sparse tokens field with invalid parameters
     */
    public void testSparseTokensFieldWithInvalidParameters1() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, -1, 0.4f, 0.1f, 8); }
        );
    }

    public void testSparseTokensFieldWithInvalidParameters2() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, -0.4f, 0.1f, 8); }
        );
    }

    public void testSparseTokensFieldWithInvalidParameters3() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, -0.1f, 8); }
        );
    }

    public void testSparseTokensFieldWithInvalidParameters4() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, -8); }
        );
    }

    /**
     * Test creating sparse tokens field with different method parameters
     */
    public void testSparseTokensFieldWithAdditionParameters() throws IOException {
        // Create index with sparse index setting enabled
        String indexSettings = prepareIndexSettings();
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_SPARSE_FIELD_NAME)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME)
            .startObject("parameters")
            .field("n_postings", 100) // Integer: length of posting list
            .field("summary_prune_ratio", 0.1f) // Float: alpha-prune ration for summary
            .field("cluster_ratio", 0.1f) // Float: cluster ratio
            .field("approximate_threshold", 8)
            .field("additional_parameter", 8)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String indexName = TEST_INDEX_NAME + "_method_params";
        Request request = new Request("PUT", "/" + indexName);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            mappingBuilder.toString()
        );
        request.setJsonEntity(body);
        expectThrows(IOException.class, () -> client().performRequest(request));
    }

    public void testIngestDocumentsAllSeismicPostingPruning() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5"), actualIds);
    }

    public void testIngestDocumentsMixSeismicWithRankFeatures() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 4);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            null,
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            null,
            4
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(7, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5", "3", "2", "1"), actualIds);
    }

    public void testIngestDocumentsWithAllRankFeatures() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 100);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            null,
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.4f, "2000", 0.4f), Map.of("1000", 0.5f, "2000", 0.5f)),
            null,
            4
        );

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.6f, "2000", 0.6f), Map.of("1000", 0.7f, "2000", 0.7f), Map.of("1000", 0.8f, "2000", 0.8f)),
            null,
            6
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(8, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "7", "6", "5", "4", "3", "2", "1"), actualIds);
    }

    public void testIngestDocumentsAllSeismicWithCut() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f),
                Map.of("3000", 0.0001f)
            )
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f, "3000", 64.0f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(1, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("9"), actualIds);
    }

    public void testIngestDocumentsSeismicHeapFactor() throws Exception {
        final int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, docCount, 1.0f, 0.5f, docCount);

        List<Map<String, Float>> docs = new ArrayList<>();
        float epsilon = 1e-7F;
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat() + epsilon);
            tokens.put("2000", randomFloat() + epsilon);
            tokens.put("3000", randomFloat() + epsilon);
            tokens.put("4000", randomFloat() + epsilon);
            tokens.put("5000", randomFloat() + epsilon);
            docs.add(tokens);
        }

        ingestDocumentsAndForceMerge(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            0.000001f,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) < docCount);

        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            100000,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );
        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertEquals(docCount, getHitCount(searchResults));
    }

    public void testIngestDocumentsAllSeismicWithPreFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "tree", "apple", "tree", "apple", "tree", "apple", "tree")
        );

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("7", "5", "3", "1"), actualIds);
        // filter tree
        filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "tree"));
        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        actualIds = getDocIDs(searchResults);
        assertEquals(List.of("8", "6", "4", "2"), actualIds);
    }

    public void testIngestDocumentsAllSeismicWithPostFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "apple", "apple", "apple", "apple", "apple", "apple", "tree")
        );

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            4,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(3, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        // results with k = 4 are 5, 6, 7, 8, filter results are 1, 2, 3, 4, 5, 6, 7
        // intersection of both are 5, 6, 7
        assertEquals(List.of("7", "6", "5"), actualIds);
    }

    public void testIngestDocumentsRankFeaturesWithFiltering() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 1, 0.4f, 0.5f, 100);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "tree", "apple", "tree", "apple", "tree", "apple", "tree"),
            1
        );

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertEquals(List.of("7", "5", "3", "1"), actualIds);
    }

    public void testIngestDocumentsMultipleShards() throws Exception {
        int shards = 3;
        int docCount = 20;
        // effective number of replica is capped by the number of OpenSearch nodes minus 1
        int replicas = Math.min(3, getNodeCount() - 1);
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 5, 0.4f, 0.5f, docCount, shards, replicas);

        List<Map<String, Float>> docs = new ArrayList<>();
        List<String> text = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat());
            tokens.put("2000", randomFloat());
            tokens.put("3000", randomFloat());
            tokens.put("4000", randomFloat());
            tokens.put("5000", randomFloat());
            docs.add(tokens);
            if (i % 2 == 0) {
                text.add("apple");
            } else {
                text.add("tree");
            }
        }
        List<String> routingIds = generateUniqueRoutingIds(shards);
        for (int i = 0; i < shards; ++i) {
            ingestDocuments(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs, text, i * docCount + 1, routingIds.get(i));
        }

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME, shards, replicas);
        // there are replica segments
        assertEquals(shards * (replicas + 1), getSegmentCount(TEST_INDEX_NAME));

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            9,
            Map.of("1000", 0.1f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 20);
        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) <= 15);
    }

    public void testSearchDocumentsWithTwoPhaseSearchProcessorThenThrowException() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        String twoPhaseSearchPipeline = "two-phase-search-pipeline";
        createNeuralSparseTwoPhaseSearchProcessor(twoPhaseSearchPipeline);
        updateIndexSettings(TEST_INDEX_NAME, Settings.builder().put("index.search.default_pipeline", twoPhaseSearchPipeline));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Exception exception = assertThrows(Exception.class, () -> search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10));
        assert (exception.getMessage()
            .contains(String.format(Locale.ROOT, "Two phase search processor is not compatible with [%s] field for now", SEISMIC)));
    }

    public void testSeismicWithModelInferencing() throws Exception {
        String modelId = prepareSparseEncodingModel();
        String sparseFieldName = "title_sparse"; // configured in SparseEncodingPipelineConfiguration.json
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING, 2);
        createSparseIndex(TEST_INDEX_NAME, sparseFieldName, 4, 0.4f, 0.5f, 8);
        String payload = prepareSparseBulkIngestPayload(
            TEST_INDEX_NAME,
            "title",
            null,
            List.of(),
            List.of("one", "two", "three", "four", "five", "six", "seven", "eight", "night", "ten"),
            1
        );
        bulkIngest(payload, PIPELINE_NAME);
        forceMerge(TEST_INDEX_NAME);
        waitForSegmentMerge(TEST_INDEX_NAME);

        assertEquals(10, getDocCount(TEST_INDEX_NAME));

        SparseAnnQueryBuilder annQueryBuilder = new SparseAnnQueryBuilder().queryCut(2).fieldName(sparseFieldName).heapFactor(1.0f).k(9);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().sparseAnnQueryBuilder(annQueryBuilder)
            .fieldName(sparseFieldName)
            .modelId(modelId)
            .queryText("one two");

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(2, getHitCount(searchResults));
        Set<String> actualIds = new HashSet<>(getDocIDs(searchResults));
        assertEquals(Set.of("1", "2"), actualIds);
    }

    /**
     * Test creating an index with multiple seismic fields
     */
    public void testCreateIndexWithMultipleSeismicFields() throws IOException {
        String indexName = TEST_INDEX_NAME + "_multiple_seismic";
        String field1 = "sparse_field_1";
        String field2 = "sparse_field_2";
        String field3 = "sparse_field_3";

        createIndexWithMultipleSeismicFields(indexName, List.of(field1, field2, field3));

        // Verify index exists
        assertTrue(indexExists(indexName));

        // Verify index mapping contains all sparse fields
        Map<String, Object> indexMapping = getIndexMapping(indexName);
        Map<String, Object> mappings = (Map<String, Object>) indexMapping.get(indexName);
        Map<String, Object> mappingsProperties = (Map<String, Object>) mappings.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappingsProperties.get("properties");

        // Check each sparse field exists in mapping
        assertTrue(properties.containsKey(field1));
        assertTrue(properties.containsKey(field2));
        assertTrue(properties.containsKey(field3));

        // Verify field types are sparse_tokens
        Map<String, Object> field1Config = (Map<String, Object>) properties.get(field1);
        Map<String, Object> field2Config = (Map<String, Object>) properties.get(field2);
        Map<String, Object> field3Config = (Map<String, Object>) properties.get(field3);

        assertEquals(SparseTokensFieldMapper.CONTENT_TYPE, field1Config.get("type"));
        assertEquals(SparseTokensFieldMapper.CONTENT_TYPE, field2Config.get("type"));
        assertEquals(SparseTokensFieldMapper.CONTENT_TYPE, field3Config.get("type"));
    }

    /**
     * Test indexing documents with multiple seismic fields
     */
    public void testIndexDocumentsWithMultipleSeismicFields() throws IOException {
        String indexName = TEST_INDEX_NAME + "_multiple_seismic_docs";
        String field1 = "sparse_field_1";
        String field2 = "sparse_field_2";
        String field3 = "sparse_field_3";

        createIndexWithMultipleSeismicFields(indexName, List.of(field1, field2, field3));

        // Create documents with different sparse tokens for each field using integer tokens
        Map<String, Float> tokens1 = Map.of("1000", 0.1f, "2000", 0.2f);
        Map<String, Float> tokens2 = Map.of("3000", 0.3f, "4000", 0.4f);
        Map<String, Float> tokens3 = Map.of("5000", 0.5f, "6000", 0.6f);

        // Index document with multiple sparse fields
        addSparseEncodingDoc(indexName, "1", List.of(field1, field2, field3), List.of(tokens1, tokens2, tokens3));

        // Verify document was indexed
        assertEquals(1, getDocCount(indexName));

        // Get the document and verify its content
        Map<String, Object> document = getDocById(indexName, "1");
        assertNotNull(document);

        Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertNotNull(source);

        // Verify all sparse fields are present with correct tokens
        Map<String, Object> sparseField1 = (Map<String, Object>) source.get(field1);
        Map<String, Object> sparseField2 = (Map<String, Object>) source.get(field2);
        Map<String, Object> sparseField3 = (Map<String, Object>) source.get(field3);

        assertNotNull(sparseField1);
        assertNotNull(sparseField2);
        assertNotNull(sparseField3);

        // Verify tokens in each field
        assertEquals(0.1f, ((Number) sparseField1.get("1000")).floatValue(), 0.001);
        assertEquals(0.2f, ((Number) sparseField1.get("2000")).floatValue(), 0.001);
        assertEquals(0.3f, ((Number) sparseField2.get("3000")).floatValue(), 0.001);
        assertEquals(0.4f, ((Number) sparseField2.get("4000")).floatValue(), 0.001);
        assertEquals(0.5f, ((Number) sparseField3.get("5000")).floatValue(), 0.001);
        assertEquals(0.6f, ((Number) sparseField3.get("6000")).floatValue(), 0.001);
    }

    /**
     * Test searching across multiple seismic fields
     */
    public void testSearchMultipleSeismicFields() throws IOException {
        String indexName = TEST_INDEX_NAME + "_multiple_seismic_search";
        String field1 = "sparse_field_1";
        String field2 = "sparse_field_2";

        createIndexWithMultipleSeismicFields(indexName, List.of(field1, field2));

        // Index multiple documents with different token distributions
        addSparseEncodingDoc(
            indexName,
            "1",
            List.of(field1, field2),
            List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("3000", 0.1f, "4000", 0.9f))
        );
        addSparseEncodingDoc(
            indexName,
            "2",
            List.of(field1, field2),
            List.of(Map.of("1000", 0.3f, "2000", 0.7f), Map.of("3000", 0.6f, "4000", 0.4f))
        );
        addSparseEncodingDoc(
            indexName,
            "3",
            List.of(field1, field2),
            List.of(Map.of("1000", 0.1f, "5000", 0.9f), Map.of("6000", 0.8f, "4000", 0.2f))
        );

        // Search on first field
        NeuralSparseQueryBuilder queryBuilder1 = getNeuralSparseQueryBuilder(field1, 2, 1.0f, 10, Map.of("1000", 0.5f, "2000", 0.5f));

        Map<String, Object> searchResults1 = search(indexName, queryBuilder1, 10);
        assertNotNull(searchResults1);
        assertTrue(getHitCount(searchResults1) > 0);

        // Search on second field
        NeuralSparseQueryBuilder queryBuilder2 = getNeuralSparseQueryBuilder(field2, 2, 1.0f, 10, Map.of("3000", 0.5f, "4000", 0.5f));

        Map<String, Object> searchResults2 = search(indexName, queryBuilder2, 10);
        assertNotNull(searchResults2);
        assertTrue(getHitCount(searchResults2) > 0);
    }

    /**
     * Test multiple seismic fields with different parameters
     */
    public void testMultipleSeismicFieldsWithDifferentParameters() throws IOException {
        String indexName = TEST_INDEX_NAME + "_multiple_seismic_params";

        // Create index with multiple sparse fields having different seismic parameters
        String indexSettings = prepareIndexSettings();
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("sparse_field_high_precision")
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME)
            .startObject("parameters")
            .field("n_postings", 200)
            .field("summary_prune_ratio", 0.2f)
            .field("cluster_ratio", 0.05f)
            .field("approximate_threshold", 16)
            .endObject()
            .endObject()
            .endObject()
            .startObject("sparse_field_low_precision")
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME)
            .startObject("parameters")
            .field("n_postings", 50)
            .field("summary_prune_ratio", 0.6f)
            .field("cluster_ratio", 0.2f)
            .field("approximate_threshold", 4)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("PUT", "/" + indexName);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            mappingBuilder.toString()
        );
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(indexName));

        // Index documents and verify they work with different precision settings
        addSparseEncodingDoc(
            indexName,
            "1",
            List.of("sparse_field_high_precision", "sparse_field_low_precision"),
            List.of(Map.of("1000", 0.8f, "2000", 0.2f), Map.of("3000", 0.1f, "4000", 0.9f))
        );

        assertEquals(1, getDocCount(indexName));
    }

    private List<String> getDocIDs(Map<String, Object> searchResults) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResults.get("hits");
        List<String> actualIds = new ArrayList<>();
        List<Object> hits1List = (List<Object>) hits1map.get("hits");
        for (Object hits1Object : hits1List) {
            Map<String, Object> mapObject = (Map<String, Object>) hits1Object;
            String id = mapObject.get("_id").toString();
            actualIds.add(id);
        }
        return actualIds;
    }
}
