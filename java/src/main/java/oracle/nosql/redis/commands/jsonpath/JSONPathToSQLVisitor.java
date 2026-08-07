/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands.jsonpath;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import oracle.nosql.driver.JsonParseException;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.AndExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.ArraySelectorContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.BasicExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.CompContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.FilterExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.MapSelectorContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.PathOrValContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.SegmentsContext;

public class JSONPathToSQLVisitor extends JSONPathVisitorBase<String> {

	private static final String IDX_SIZE_PFX = "size($) + ";

	// Note that in JSON Path, regex pattern itself may be a path and not a
	// literal, so we in general we cannot preprocess it on the client side.
	// The following expressions allow some rudimentary preprocessing inside
	// a query.

	// This expression accounts for difference in behavior where SQL
	// regex_like() matches only the whole string, but JSON Path regex matches
	// a substring. It also introduces support for ^ and $ markers, which must
	// be in the beginning and end of the pattern respectively (but after
	// flags, see below).
	private static final String REGEX_PATTERN_TRANSFORM1 =
		"CASE WHEN NOT starts_with($, '^') AND NOT ends_with($, '$') " +
		"THEN '.*' || $ || '.*' " +
		"WHEN starts_with($, '^') AND NOT ends_with($, '$') " +
		"THEN substring($, 1) || '.*' " +
		"WHEN NOT starts_with($, '^') AND ends_with($, '$') " +
		"THEN '.*' || substring($, 0, length($) - 1) " +
		// starts with ^ and ends with $
		"ELSE substring($, 1, length($) - 2) END";

	// These 2 expressions allow to specify inline flags in format "(?flags)",
	// (see https://docs.rs/regex/latest/regex/#syntax) we only support it at
	// the very beginning of the pattern (even before ^). Supported flags are
	// i, s, u, x. Note that R is enabled by default and u is not. Clearing of
	// flags is also not supported.
	private static final String REGEX_PATTERN_TRANSFORM2 =
		"CASE WHEN NOT starts_with($, '(?') THEN $ ELSE " +
		"substring($, index_of($, ')') + 1) END";
	private static final String REGEX_MODE_TRANSFORM =
		"CASE WHEN NOT starts_with($, '(?') THEN '' ELSE " +
		"substring($, 2, index_of($, ')') - 2) END";

	// Expression that combines support for ^, $ and the flags and allows
	// patterns like "(?flags)^pattern$" (all of the markers are optional).
	// Note that currently regex_like() casts its arguments to strings, so
	// to conform to JSON Path, we check the arg types first.
	private static final String REGEX_FMT =
		"(%s IS OF TYPE (String) AND %s IS OF TYPE (String) AND " +
		"seq_transform(%s, regex_like(%s, seq_transform(%s, %s), %s)))";

	private final String root;
	private final HashMap<String, FieldValue> variables = new HashMap<>();
	// In JSONPath, bracketed expression can contain multiple selectors
	// separated by ",". Result of parsing of each selector may depend on
	// whether there is a single selector or multiple, so we keep this info on
	// the stack.
	private final Deque<Boolean> isSingleSelector = new ArrayDeque<>();

	// For JSON.SET, we have to differentiate between updating of existing
	// values and setting new values (by creating new key in an object),
	// because in SQL we have to use UPDATE with SET clause for the former PUT
	// clause for the latter (because SET clause cannot be used to create new
	// keys). Note that we cannot create new key for any arbitrary JSON Path
	// expression. E.g. paths that end in filter, wildcard or slice
	// expression (e.g. $.x[?(@.y > 1)], $.x.*, $.x[1:5]) can only reference
	// existing keys or array elements. Specifically, we can only create new
	// key(s) if the path ends in a concrete field name, or multiple field
	// names in case of brackets with multiple selectors. E.g. $.x.y,
	// $.x[?(@...)].y, $.x.*.y, $.x["y","z","h"].
	// In these limited cases, it is ok to use PUT clause, since it can be
	// used both to create new key-values or replace existing values.
	// For this we need the following: field name or names to construct
	// new-value expression for the PUT clause and also the parent path, which
	// will serve as the target expression in the PUT clause.
	// To detect when this is applicable, we look at DotSegment or
	// BracketSegment nodes that have no following "segments" child node (see
	// "segments?" in the grammar). Then we can collect field names from
	// either FieldId (under DotSegment) or MapSelector (under BracketSegment).
	// If at the end of traversal we get non-empty leafFields list, new fields
	// can be created with this path and we can use PUT clause for this.
	// Otherwise, new fields cannot be created and we use SET clause.
	// Note that we also need to exclude cases when we are inside a filter
	// since filter can contain arbitrary JSON Path expressions.
	// Similar approach is also used for JSON.MERGE, since in SQL JSON merge
	// patch the target cannot reference non-existing value. For this, we
	// merge with the parent path instead, but the patch object used for this
	// will be an object containing leafFields with values assigned to the
	// user-provided patch.
	private ArrayList<String> leafFields = new ArrayList<>(4);
	private String sqlPath;

	// lastSegment keeps track of the last segment in the sqlPath. Knowing
	// last segment's length allows us to get the parent of the sqlPath.
	private String lastSegment;

	// Additional filter to apply to every resulting value. Currently used
	// for array operations to filter only values that are arrays. The format
	// is the same as for SQL map filter expressions.
	private String endValueFilter;

	// Additional filter to apply to parent nodes of resulting values.
	// Currently used for JSON.DEL command where we need to perform cleanup
	// on any parent array.
	private String parentFilter;

	private boolean inParentSegment;

	public JSONPathToSQLVisitor(String root) {
		this.root = root;
	}

	private static boolean isLastSegment(ParserRuleContext ctx) {
		return ctx.getChild(SegmentsContext.class, 0) == null;
	}

	private static boolean isParentSegment(ParserRuleContext ctx) {
		ParserRuleContext seg = ctx.getChild(SegmentsContext.class, 0);
		return seg != null && isLastSegment(seg);
	}

	private String addVariable(FieldValue val) {
		String name = "$var" + variables.size();
		variables.put(name, val);
		return name;
	}

	private String applyWildcard() {
		String filter = getAdditionalFilter();
		return filter != null ?
			String.format("values(%s)", filter) : "values()";
	}

	private String getAdditionalFilter() {
		// We must not apply any additional filter if we are inside JSON path
		// filter expression, additional filters have to be applied only
		// outside JSON path filters. Note that checking inLastSegment is not
		// enough since the last segment can be the JSON path filter.
		if (inFilter()) {
			return null;
		}
		if (inParentSegment) {
			return parentFilter;
		}
		if (inLastSegment) {
			return endValueFilter;
		}
		return null;
	}

	private String getRootPathWithFilter(String filter) {
		int i = root.lastIndexOf('.');
		assert i >= 0;
		String rootPar = root.substring(0, i);
		String rootField = root.substring(i + 1);
		return String.format("%s.values($key = '%s' AND (%s))", rootPar,
			rootField, filter);
	}

	private String makeRegexLike(String path, String pattern) {
		return String.format(REGEX_FMT, path, pattern, pattern, path,
		REGEX_PATTERN_TRANSFORM2, REGEX_PATTERN_TRANSFORM1,
		REGEX_MODE_TRANSFORM);
	}

	protected String aggregateResult(String aggr, String res) {
		return aggr + res;
	}

	protected String defaultResult() {
		return "";
	}

	public void setEndValueFilter(String endValueFilter) {
		this.endValueFilter = endValueFilter;
	}

	public void setParentFilter(String parentFilter) {
		this.parentFilter = parentFilter;
	}

	public Map<String, FieldValue> getVariables() {
		return variables;
	}

	public String getSQLPath() {
		return sqlPath;
	}

	public String getParentSQLPath() {
		if (lastSegment == null) {
			return "";
		}

		assert(sqlPath.endsWith(lastSegment));
		return sqlPath.substring(0,	sqlPath.length() - lastSegment.length());
	}

	// Return null if there are no leaf fields.
	public List<String> getLeafFields() {
		return !leafFields.isEmpty() ? leafFields : null;
	}

	public void run(ParseTree parseTree, String input) {
		lastSegment = null;
		leafFields.clear();
		assert inFilterCnt == 0;
		assert !inLastSegment;
		assert !inParentSegment;

		assert input != null;
		this.input = input;
		sqlPath = this.visit(parseTree);
	}

	@Override
	public String visitRootPath(JSONPathParser.RootPathContext ctx) {
		// Note that since root path is a common case, we handle it outside in
		// JSONCommandsBase.translatePath...() methods to avoid invoking the
		// parser.
		assert(!isLastSegment(ctx));

		if (parentFilter != null && isParentSegment(ctx)) {
			return getRootPathWithFilter(parentFilter) + visitChildren(ctx);
		}

		return root + visitChildren(ctx);
	}
	
	@Override
	public String visitAnyPath(JSONPathParser.AnyPathContext ctx) {
		return (ctx.DOLLAR() != null ? root : "$value") + visitChildren(ctx);
	}
	
	@Override
	public String visitJsonValue(JSONPathParser.JsonValueContext ctx) {
        try {
			return addVariable(FieldValue.createFromJson(ctx.getText(), null));
        } catch(JsonParseException ex) {
            throw parseException(ctx, "Invalid JSON value: " + ctx.getText(),
				ex);
        }
	}

	@Override
	public String visitDotSegment(JSONPathParser.DotSegmentContext ctx) {
		if (!inFilter()) {
			assert(!inLastSegment && lastSegment == null);
			if (isLastSegment(ctx)) {
				inLastSegment = true;
				leafFields.clear();
			} else if (parentFilter != null && isParentSegment(ctx)) {
				inParentSegment = true;
			}
		}
		String res = '.' + visitChildren(ctx);
		if (!inFilter()) {
			if (inLastSegment) {
				lastSegment = res;
				inLastSegment = false;
			} else {
				inParentSegment = false;
			}
		}
		return res;
	}

	@Override
	public String visitBracketsSegment(
		JSONPathParser.BracketsSegmentContext ctx) {
		if (!inFilter()) {
			assert(!inLastSegment && lastSegment == null);
			if (isLastSegment(ctx)) {
				inLastSegment = true;
				leafFields.clear();
			} else if (parentFilter != null && isParentSegment(ctx)) {
				inParentSegment = true;
			}
		}
		String res = visitChildren(ctx);
		if (!inFilter()) {
			if (inLastSegment) {
				lastSegment = res;
				inLastSegment = false;
			} else {
				inParentSegment = false;
			}
		}
		return res;
	}

	@Override
	public String visitDescendantSegment(
		JSONPathParser.DescendantSegmentContext ctx) {
		throw parseException(ctx, "Recursive descent is not supported");
	}

	@Override
	public String visitFieldId(JSONPathParser.FieldIdContext ctx) {
		String res = ctx.getText();
		String filter = getAdditionalFilter();
		if (filter != null || inLastSegment) {
			String fieldVar = addVariable(new StringValue(res));
			if (inLastSegment) {
				leafFields.add(fieldVar);
			}
			if (filter != null) {
				return String.format("values($key = %s AND (%s))", fieldVar,
					filter);
			}
		}

		// Waiting to resolution of issue of using external variables with the
		// field step. For us, it is better to bind a variable rather than use
		// identifier directly because then the same prepared statement can be
		// shared among multiple similar queries (see PreparedStatementCache).

		// We quote the field name in case it is not a proper SQL identifier
		// name (starts with a letter and contains only letters, numbers and
		// underscore).
		return quote(res);
	}
	
	@Override
	public String visitWildcard(JSONPathParser.WildcardContext ctx) {
		return applyWildcard();
	}
	
	@Override
	public String visitArraySelectors(
		JSONPathParser.ArraySelectorsContext ctx) {
		String filter = getAdditionalFilter();
		String selSfx = filter != null ?
			String.format("].values($key = 'v' AND (%s))", filter) :
			"].v";

		List<ArraySelectorContext> selectors = ctx.arraySelector();
		int cnt = selectors.size();
		
		if (cnt == 1) {
			isSingleSelector.push(true);
			String res = '[' + visit(selectors.get(0)) + selSfx;
			isSingleSelector.pop();
			return res;
		}
		
		isSingleSelector.push(false);
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for(int i = 0; i < cnt; i++) {
			sb.append('(');
			sb.append(visit(selectors.get(i)));
			sb.append(')');
			if (i < cnt - 1) {
				sb.append(" OR ");
			}
		}
		sb.append(selSfx);

		isSingleSelector.pop();
		return sb.toString();
	}

	@Override
	public String visitMapSelectors(JSONPathParser.MapSelectorsContext ctx) {
		List<MapSelectorContext> selectors = ctx.mapSelector();
		int cnt = selectors.size();
		String filter = getAdditionalFilter();

		if (cnt == 1 && filter == null) {
			isSingleSelector.push(true);
			// Use field step if possible since it is faster than map filter,
			// see visitMapSelector().
			String res = '.' + visit(selectors.get(0));
			isSingleSelector.pop();
			return res;
		}
		
		isSingleSelector.push(false);
		StringBuilder sb = new StringBuilder();
		sb.append(".values(");
		if (filter != null) {
			sb.append('(');
		}
		for(int i = 0; i < cnt; i++) {
			sb.append('(');
			sb.append(visit(selectors.get(i)));
			sb.append(')');
			if (i < cnt - 1) {
				sb.append(" OR ");
			}
		}
		if (filter != null) {
			// If filter filter is applied, we get this kind of result:
			// .values(($key = ... OR $key = ... OR ...) AND (<filter>))
			// so the same filter is applied to all map selectors.
			sb.append(String.format(") AND (%s)", filter));
		}
		sb.append(')');

		isSingleSelector.pop();
		return sb.toString();
	}
	
	@Override
	public String visitWildcardSelector(
		JSONPathParser.WildcardSelectorContext ctx) {
		return '.' + applyWildcard();
	}

	@Override
	public String visitFilterSelector(
		JSONPathParser.FilterSelectorContext ctx) {
		String extraFilter = getAdditionalFilter();

		inFilterCnt++;
		String filterRes = visit(ctx.filterExpr());
		if (extraFilter != null) {
			filterRes = String.format("(%s) AND (%s)", filterRes,
				extraFilter);
		}
		
		String res = ".values(" + filterRes + ")";
		inFilterCnt--;
		
		return res;
	}
	
	@Override
	public String visitIndexSelector(JSONPathParser.IndexSelectorContext ctx) {
		IntegerValue val;
		try {
			val = new IntegerValue(ctx.NUMBER().getText());
		} catch(NumberFormatException ex) {
			throw parseException(ctx, "Invalid array index: " +
				ctx.NUMBER().getText(), ex);
		}

		String name = addVariable(val);
		String indexStr = val.getValue() >= 0 ? name : IDX_SIZE_PFX + name;
		assert !isSingleSelector.isEmpty();
		return isSingleSelector.peek() ? indexStr : "$pos = " + indexStr;
	}
	
	@Override public String visitSliceSelector(
		JSONPathParser.SliceSelectorContext ctx) {
		IntegerValue start = null;
		IntegerValue end = null;
		IntegerValue step = null;

		int cnt = ctx.getChildCount();
		assert cnt > 0;
		int argIdx = 0;
		for(int i = 0; i < cnt; i++) {
			ParseTree node = ctx.getChild(i);
			assert node instanceof TerminalNode;
			if (((TerminalNode)node).getSymbol().getType() ==
				JSONPathParser.COLON) {
				argIdx++;
				continue;
			}

			assert ((TerminalNode)node).getSymbol().getType() ==
				JSONPathParser.NUMBER;
			IntegerValue val;
			try {
				val = new IntegerValue(node.getText());
			} catch(NumberFormatException ex) {
				throw parseException(ctx,
					"Invalid slice argument: " + node.getText(), ex);
			}

			switch(argIdx) {
				case 0:
					start = val;
					break;
				case 1:
					end = val;
					break;
				case 2:
					step = val;
					if (step.getValue() <= 0) {
						throw parseException(ctx,
							"Slice step must be positive");
					};
					break;
				default:
					assert(false);
			}
		}

		String startStr = "";
		String endStr = "";

		if (start != null) {
			startStr = addVariable(start);
			// SQL does not support negative indexes, so we have to convert.
			if (start.getValue() < 0) {
				startStr = IDX_SIZE_PFX + startStr;
			}
		}
		if (end != null) {
			endStr = addVariable(end);
			if (end.getValue() < 0) {
				endStr = IDX_SIZE_PFX + endStr;
			}	
		}

		// SQL array slice step does not currently support "step" parameter.
		// If step is not specified, and there is only one selector, we can
		// use SQL array slice step.
		assert !isSingleSelector.isEmpty();
		if (step == null && isSingleSelector.peek()) {
			// Unlike in JSONPath, in SQL slice expression, the end bound is
			// inclusive, so we adjust it.
			if (end != null) {
				endStr += " - 1";
			}
			return startStr + ":" + endStr;
		}

		// Otherwise we have to use SQL map filter step.
		StringBuilder sb = new StringBuilder();

		if (start != null) {
			sb.append("$pos >= ").append(startStr);
		}
		
		if (end != null) {
			if (sb.length() != 0) {
				sb.append(" AND ");
			}
			sb.append("$pos < ").append(endStr);
		}

		if (step != null) {
			String stepStr = addVariable(step);
			if (sb.length() != 0) {
				sb.append(" AND ");
			}

			// Offset from the start, if start is specified, or just position
			// if start is not specified.
			String offsetStr = start != null ?
				String.format("($pos - (%s))", startStr) : "$pos";

			// Unfortunately modulo expression is not supported, so we have to
			// do it manually using integer division ("/") as:
			// "(offset / step) * step = offset".
			sb.append(String.format("(%s / %s) * %s = %s", offsetStr, stepStr,
				stepStr, offsetStr));
		}

		return sb.toString();
	}
	
	@Override
	public String visitMapSelector(JSONPathParser.MapSelectorContext ctx) {
		assert !isSingleSelector.isEmpty();
		// For single selector use field step rather than map filter.
		// Currently, there is a problem with using external variables with
		// field step in some cases, waiting for resolution.
		String key = isSingleSelector.peek() ?
			ctx.getText() : addVariable(new StringValue(unquote(
				ctx.getText())));
		if (inLastSegment) {
			leafFields.add(key);
		}
		return isSingleSelector.peek() ? key : "$key = " + key;
	}
	
	@Override
	public String visitFilterExpr(JSONPathParser.FilterExprContext ctx) {
		AndExprContext andExpr = ctx.andExpr();
		assert andExpr != null;
		FilterExprContext filterExpr = ctx.filterExpr();
		// We rely on the fact that in SQL "OR" had lower presedence than
		// "AND".
		return filterExpr == null ?
			visit(andExpr) : visit(andExpr) + " OR " + visit(filterExpr);
	}
	
	@Override
	public String visitAndExpr(JSONPathParser.AndExprContext ctx) {
		BasicExprContext basicExpr = ctx.basicExpr();
		assert basicExpr != null;
		AndExprContext andExpr = ctx.andExpr();
		// We rely on the fact that in SQL "AND" had lower precedence than
		// any constructs in basic expr.
		return andExpr == null ?
			visit(basicExpr) : visit(basicExpr) + " AND " + visit(andExpr);
	}
	
	@Override
	public String visitParenExpr(JSONPathParser.ParenExprContext ctx) {
		String innerRes = '(' + visit(ctx.filterExpr()) + ')';
		return ctx.NOT() == null ? innerRes : "NOT " + innerRes;
	}

	@Override
	public String visitComparisonExpr(
		JSONPathParser.ComparisonExprContext ctx) {
		List<PathOrValContext> vals = ctx.pathOrVal();
		int cnt = vals.size();
		assert cnt >= 1 && cnt <= 2;
		if (cnt == 1) {
			assert ctx.comp() == null;
			// It seems that Redis JSON follows RFC 9535, which in this case
			// only test the existence of the item designated by the embedded
			// path rather than its Javascript boolean conversion, so the
			// values such as null, false, 0 and "" will pass the test.
			return String.format("(EXISTS (%s))", visit(vals.get(0)));
		}

		CompContext comp = ctx.comp();
		assert comp != null;
		assert comp.getChildCount() == 1;
		assert comp.getChild(0) instanceof TerminalNode;
		TerminalNode tComp = (TerminalNode)comp.getChild(0);
		String compStr = null;
		switch(tComp.getSymbol().getType()) {
			case JSONPathParser.EQ:
				compStr = "=";
				break;
			case JSONPathParser.NE:
				compStr = "!=";
				break;
			case JSONPathParser.LT:
				compStr = "<";
				break;
			case JSONPathParser.GT:
				compStr = ">";
				break;
			case JSONPathParser.LE:
				compStr = "<=";
				break;
			case JSONPathParser.GE:
				compStr = ">=";
				break;
			case JSONPathParser.MATCH:
				return makeRegexLike(visit(vals.get(0)), visit(vals.get(1)));
			default:
				assert(false);
				break;
		}

		return String.format("(%s) %s (%s)", visit(vals.get(0)), compStr,
			visit(vals.get(1)));
	}

}
