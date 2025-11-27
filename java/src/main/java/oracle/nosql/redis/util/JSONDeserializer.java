/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.util;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import java.io.IOException;
import java.math.BigDecimal;

import com.fasterxml.jackson.core.JsonLocation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import oracle.nosql.driver.values.*;
import oracle.nosql.redis.RedisResponseException;

public class JSONDeserializer {

    private static final JsonFactory factory =
        JsonFactory.builder()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
            .build();

    private static RedisResponseException parseException(String msg,
        JsonParser jp, Throwable cause) {
        JsonLocation loc = jp != null ? jp.getCurrentLocation() : null;
        if (loc == null || loc == JsonLocation.NA ||
            (loc.getLineNr() < 0 && loc.getColumnNr() < 0)) {
            return new RedisResponseException(msg, cause);
        }

        int lineNr = loc.getLineNr();
        int colNr = loc.getColumnNr();

        String locStr = " at" +
            (lineNr >= 0 ? " line " + lineNr: "") +
            (colNr >= 0 ? " column " + colNr: "");

        return new RedisResponseException(msg + locStr, cause);
    }

    private static RedisResponseException parseException(String msg,
        JsonParser jp) {
        return parseException(msg, jp, null);
    }

    private static FieldValue parseObject(JsonParser jp)
        throws RedisResponseException, IOException {
        // In JSON, the order of keys in object is not guaranteed. It is not
        // clear to me if there is an "expectation" in Redis JSON of
        // predictable order of keys, but the observed behavior seems to
        // confirm that and the existing tests in RedisJSON repo expect that.
        // It is probably not much overhead to use MapValue with maintained
        // insertion order (16 is the default capacity of underlying
        // LinkedHashMap object).
        MapValue map = new MapValue(true, 16);

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            if (token == null || fieldName == null) {
                throw parseException("key must be a string", jp);
            }

            // true tells the method to fetch the next token
            FieldValue field = createValueFromJson(jp, true);
            map.put(fieldName, field);
        }
        return map;
    }

    private static FieldValue parseArray(JsonParser jp)
        throws IOException, RedisResponseException {
        ArrayValue array = new ArrayValue();

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            if (token == null) {
                throw parseException("expected value", jp);
            }

            // false means don't get the next token, it's been fetched
            array.add(createValueFromJson(jp, false));
        }
        return array;
    }

    private static FieldValue createValueFromJson(JsonParser jp,
        boolean getNext) throws IOException, RedisResponseException {
        JsonToken token =
            (getNext ? jp.nextToken() : jp.getCurrentToken());
        if (token == null) {
            throw parseException("EOF while parsing value", jp);
        }

        switch (token) {
            case VALUE_STRING:
                return new StringValue(jp.getText());
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                JsonParser.NumberType numberType = jp.getNumberType();
                switch (numberType) {
                    case INT:
                        return new IntegerValue(jp.getIntValue());
                    case LONG:
                        return new LongValue(jp.getLongValue());
                    case BIG_INTEGER:
                    case BIG_DECIMAL:
                        BigDecimal bd = jp.getDecimalValue();
                        assert bd != null;
                        return new DoubleValue(bd.doubleValue());
                    case FLOAT:
                    case DOUBLE:
                        return new DoubleValue(jp.getDoubleValue());
                    default:
                        throw parseException("unexpected numeric type: " +
                            numberType, jp);
                }
        case VALUE_TRUE:
            return BooleanValue.trueInstance();
        case VALUE_FALSE:
            return BooleanValue.falseInstance();
        case VALUE_NULL:
            return JsonNullValue.getInstance();
        case START_OBJECT:
            return parseObject(jp);
        case START_ARRAY:
            return parseArray(jp);
        case FIELD_NAME:
        case END_OBJECT:
        case END_ARRAY:
        default:
            throw parseException(
                "unexpected token while parsing value: " + token, jp);
        }
    }

    public static FieldValue deserialize(String jsonStr)
        throws RedisResponseException {
        assert jsonStr != null;
        JsonParser jp;
        try {
            jp = factory.createParser(jsonStr);
        } catch (IOException ex) {
            throw new RedisResponseException(ex.getMessage(), ex);
        }

        try (jp) {
            return createValueFromJson(jp, true);
        } catch (IOException ex) {
            throw parseException(ex.getMessage(), jp, ex);
        }
    }

}
