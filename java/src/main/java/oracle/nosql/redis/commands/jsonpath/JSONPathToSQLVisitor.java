package oracle.nosql.redis.commands.jsonpath;
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
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathBaseVisitor;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.AndExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.ArraySelectorContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.BasicExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.BracketsSegmentContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.CompContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.DotSegmentContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.FilterExprContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.MapSelectorContext;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser.PathOrValContext;
import oracle.nosql.redis.util.Utils;

public class JSONPathToSQLVisitor extends JSONPathBaseVisitor<String> {

	private static final String IDX_SIZE_PFX = "size($) + ";

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
	//expression (e.g. $.x[?(@.y > 1)], $.x.*, $.x[1:5]) can only reference
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
	// If at the end of traversal we get non-empty endFields list, new fields
	// can be created with this path and we can use PUT clause for this.
	// Otherwise, new fields cannot be created and we use SET clause.
	// Note that we also need to exclude cases when we are inside a filter
	// since filter can contain arbitrary JSON Path expressions.
	private ArrayList<String> putFields;
	private String input; // for error reporting
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

	// Note that since filter can contain arbitrary JSON Path expressions, it
	// is possible for filters to be nested.
	private int inFilterCnt;
	private boolean inLastSegment;
	private boolean inParentSegment;

	public JSONPathToSQLVisitor(String root) {
		this.root = root;
	}

	private static String unquote(String s) {
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

	private static boolean isLastSegment(DotSegmentContext seg) {
		int cnt = seg.getChildCount();
		assert(cnt == 3 || cnt == 2);
		return cnt == 2;
	}

	private static boolean isLastSegment(BracketsSegmentContext seg) {
		int cnt = seg.getChildCount();
		assert(cnt == 2 || cnt == 1);
		return cnt == 1;
	}

	private static boolean isLastSegment(ParseTree seg) {
		if (seg instanceof DotSegmentContext) {
			return isLastSegment((DotSegmentContext)seg);
		}
		if (seg instanceof BracketsSegmentContext) {
			return isLastSegment((BracketsSegmentContext)seg);
		}
		assert(false);
		return false;
	}

	private static boolean isParentSegment(ParseTree seg) {
		int cnt = seg.getChildCount();
		assert(cnt > 0);
		return isLastSegment(seg.getChild(cnt - 1));
	}

	private String addVariable(FieldValue val) {
		String name = "$var" + variables.size();
		variables.put(name, val);
		return name;
	}

	private boolean inFilter() {
		assert inFilterCnt >= 0;
		return inFilterCnt > 0;
	}

	private RuntimeException parseException(ParserRuleContext ctx,
		String msg, Throwable cause) {
		return Utils.parseException(input,
			ctx.getStart().getCharPositionInLine(), msg, cause);
	}

	private RuntimeException parseException(ParserRuleContext ctx,
		String msg) {
		return parseException(ctx, msg, null);
	}

	private String applyWildcard() {
		String filter = getAdditionalFilter();
		return filter != null ?
			String.format("values(%s)", filter) : "values()";
	}

	// Just for completeness. More efficient to bypass visitor entirely for
	// for this case.
	private String getRootEndValueFilter() {
		assert endValueFilter != null;
		int i = root.lastIndexOf('.');
		assert i >= 0;
		String rootPar = root.substring(0, i);
		String rootField = root.substring(i + 1);
		return String.format("%s.values($key = '%s' AND (%s))", rootPar,
			rootField, endValueFilter);
	}

	private String getAdditionalFilter() {
		if (inParentSegment) {
			return parentFilter;
		}
		if (inLastSegment) {
			return endValueFilter;
		}
		return null;
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

	public void setIsForSet(boolean isForSet) {
		putFields = isForSet ? new ArrayList<>() : null;
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

	public List<String> getPutFields() {
		return putFields != null && !putFields.isEmpty() ? putFields : null;
	}

	public String getSQLPutPath() {
		return getPutFields() != null ? getParentSQLPath() : null;
	}

	public void run(ParseTree parseTree, String input) {
		if (putFields != null) {
			putFields.clear();
			lastSegment = null;
		}
		assert(inFilterCnt == 0);
		assert(!inLastSegment);

		assert input != null;
		this.input = input;
		sqlPath = this.visit(parseTree);
	}

	@Override
	public String visitRootPath(JSONPathParser.RootPathContext ctx) {
		if (endValueFilter != null && ctx.getChildCount() == 1) {
			return getRootEndValueFilter();
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
		assert(!inLastSegment && lastSegment == null);
		if (!inFilter()) {
			if (isLastSegment(ctx)) {
				inLastSegment = true;
				if (putFields != null) {
					putFields.clear();
				}
			} else if (parentFilter != null && isParentSegment(ctx)) {
				inParentSegment = true;
			}
		}
		String res = '.' + visitChildren(ctx);
		if (inLastSegment) {
			lastSegment = res;
			inLastSegment = false;
		} else {
			inParentSegment = false;
		}
		return res;
	}

	@Override
	public String visitBracketsSegment(
		JSONPathParser.BracketsSegmentContext ctx) {
		assert(!inLastSegment && lastSegment == null);
		if (!inFilter()) {
			if (isLastSegment(ctx)) {
				inLastSegment = true;
				if (putFields != null) {
					putFields.clear();
				}
			} else if (parentFilter != null && isParentSegment(ctx)) {
				inParentSegment = true;
			}
		}
		String res = visitChildren(ctx);
		if (inLastSegment) {
			lastSegment = res;
			inLastSegment = false;
		} else {
			inParentSegment = false;
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
		boolean addPutField = inLastSegment && putFields != null;
		if (filter != null || addPutField) {
			String fieldVar = addVariable(new StringValue(res));
			if (addPutField) {
				putFields.add(fieldVar);
			}
			if (filter != null) {
				return String.format("values($key = %s AND (%s))", fieldVar,
					filter);
			}
		}
		return res;
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
		inFilterCnt++;
		String filterRes = visit(ctx.filterExpr());
		String extraFilter = getAdditionalFilter();
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
		// For single selector it is faster to use field step rather than
		// map filter, however in this case we cannot bind the key to a
		// variable and have to use a literal.
		String key = isSingleSelector.peek() ?
			ctx.getText() : addVariable(new StringValue(unquote(
				ctx.getText())));
		if (inLastSegment && putFields != null) {
			putFields.add(key);
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
		// We rely on the fact that in SQL "AND" had lower presedence than
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
			default:
				assert(false);
				break;
		}

		return String.format("(%s) %s (%s)", visit(vals.get(0)), compStr,
			visit(vals.get(1)));
	}

}
