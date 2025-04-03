/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

public class PartitionedSparseQueryParser implements QueryParser {
    @Override
    public String[] names() {
        return new String[] { "partitioned_sparse" };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        Map<String, Float> queryTokens = new HashMap<>();
        List<Integer> clusterIds = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("tokens".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String tokenName = parser.currentName();
                        parser.nextToken();
                        float weight = parser.floatValue();
                        queryTokens.put(tokenName, weight);
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("clusters".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        clusterIds.add(parser.intValue());
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    fieldName = parser.text();
                }
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No field specified for partitioned_sparse query");
        }

        if (queryTokens.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "No tokens specified for partitioned_sparse query");
        }

        if (clusterIds.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "No clusters specified for partitioned_sparse query");
        }

        return new PartitionedSparseQuery(fieldName, queryTokens, clusterIds);
    }
}
