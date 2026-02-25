/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.commands.jsonpath;

import java.util.*;
import java.util.regex.Pattern;

import oracle.nosql.driver.values.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import oracle.nosql.driver.JsonParseException;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathBaseVisitor;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.AndExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.BasicExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.CompContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.FilterExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.PathOrValContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.SegmentsContext;
import oracle.nosql.redis.util.Utils;

public class JSONPathVisitorBase<T> extends JSONPathBaseVisitor<T> {

	protected String input; // for error reporting

	// Note that since filter can contain arbitrary JSON Path expressions, it
	// is possible for filters to be nested.
	protected int inFilterCnt;
	protected boolean inLastSegment;

	protected static String quote(String s) {
		return '"' + s.replace("\"", "\\\"") + '"';
	}

	protected static String unquote(String s) {
		assert s != null && s.length() >= 2;
		String quote = null;
		if (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
			quote = "\"";
		} else if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
			quote = "'";
		} else {
			// The grammar should disallow any other cases.
			assert false;
		}

		// We need to take care of escape sequences.
		s = s.substring(1, s.length() - 1).replace("\\" + quote, quote);
		// The grammar would not allow unescaped quotes in the string.
		assert !s.contains(quote);

		return s;
	}

	protected boolean inFilter() {
		assert inFilterCnt >= 0;
		return inFilterCnt > 0;
	}

	protected RuntimeException parseException(ParserRuleContext ctx,
		String msg, Throwable cause) {
		return Utils.parseException(input,
			ctx.getStart().getCharPositionInLine(), msg, cause);
	}

	protected RuntimeException parseException(ParserRuleContext ctx,
		String msg) {
		return parseException(ctx, msg, null);
	}

}
