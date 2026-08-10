/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands.jsonpath;

import java.util.*;
import java.util.regex.Pattern;

import oracle.nosql.driver.values.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import oracle.nosql.driver.JsonParseException;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser.AndExprContext;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser.BasicExprContext;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser.CompContext;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser.FilterExprContext;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser.PathOrValContext;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser.SegmentsContext;

// This visitor allows both getting and setting value indicated by JSON path
// provided via the parse tree.

public class JSONPathValueVisitor extends JSONPathVisitorBase<FieldValue> {

	private static final ArrayValue EMPTY_ARRAY_VALUE = new ArrayValue(0);

	private FieldValue rootVal;
	// Not null when we are setting new value (see jsonSet) and null when we
	// are getting value (see jsonGet).
	private FieldValue valToSet;

	// We keep the current value with which we are working on top of the stack.
	private final Deque<FieldValue> stack = new ArrayDeque<>(4);

	private static boolean filterCanCompare(FieldValue val0, FieldValue val1) {
		if (val0.isNumeric()) {
			return val1.isNumeric();
		}
		return (val0.getType() == val1.getType()) &&
			(val0.isString() || val0.isBoolean());
	}

	private static int filterCompare(FieldValue val0, FieldValue val1) {
		if (val0.getType() == val1.getType()) {
			return val0.compareTo(val1);
		}
		if (val0.isNumber() || val1.isNumber()) {
			return val0.getNumber().compareTo(val1.getNumber());
		}
		if (val0.isDouble() || val1.isDouble()) {
			return Double.compare(val0.getDouble(), val1.getDouble());
		}
		return Long.compare(val0.getLong(), val1.getLong());
	}

	private static boolean filterEquals(FieldValue val0, FieldValue val1) {
		if (val0.getType() == val1.getType()) {
			return val0.equals(val1);
		}
		// We don't need cases for NULL vs JSON NULL since there could be only
		// JSON NULL values in JSON column.
		if (!val0.isNumeric() || !val1.isNumeric()) {
			return false;
		}
		if (val0.isNumber() || val1.isNumber()) {
			return val0.getNumber().equals(val1.getNumber());
		}
		if (val0.isDouble() || val1.isDouble()) {
			return val0.getDouble() == val1.getDouble();
		}
		return val0.getLong() == val1.getLong();
	}

	// Used when setting valToSet inside rootVal. Some JSON paths will require
	// valToSet to be set in multiple locations inside rootVal. This is
	// problematic if valToSet is an array or a map. One problem it would cause
	// is with transform/untransform (see JSONCommandsBase). In general, it is
	// problematic to set reference to the same mutable value inside multiple
	// locations in rootVal, so we make a copy. Atomic FieldValue instances are
	// immutable, so we use them as is (no copy is needed).
	private static FieldValue copyValue(FieldValue val) {
		if (val.isAtomic()) {
			return val;
		}

		if (val.isArray()) {
			ArrayValue res = new ArrayValue(val.asArray().size());
			for(FieldValue elem : val.asArray()) {
				res.add(copyValue(elem));
			}
			return res;
		}

		assert val.isMap();
		MapValue res = new MapValue(val.asMap().size());
		for(Map.Entry<String, FieldValue> entry : val.asMap().entrySet()) {
			res.put(entry.getKey(), copyValue(entry.getValue()));
		}
		return res;
	}

	// The visit... methods will return two kind of values: BooleanValue when
	// setting new value and ArrayValue when getting values. The BooleanValue
	// will be true if at least one value indicated by the JSON path has been
	// set and false otherwise. The ArrayValue will contain array of values
	// returned by given JSON path component (this is the case even when a
	// given path component can only return a single value). The setting of
	// value occurs only when we are in the last segment and not in filter.
	// Note that inLastSegment is set only outside of filter. If the last
	// segment is a filter, once we enter the filter, even though inLastSegment
	// is true, it will not have effect inside the filter. For any value
	// indicated by any path inside a filter expression, we need only to get
	// that value. Note that the visiting of a filter expression returns a
	// boolean which is then used to filter out values in
	// visitFilterSelector().
	// This implementation could be optimized regarding arrays, since in many
	// cases selectors return at most single value. This would involve using a
	// separate class for return value of the visitor to consider possible
	// cases, instead of FieldValue. Or we could use a separate array to
	// accumulate the results instead of using visitor return values (this
	// would be more complex for cases when we have filter inside a filter
	// inside a filter etc.)

	private boolean toSetVal() {
		return inLastSegment && valToSet != null && !inFilter();
	}

	private FieldValue emptyResult() {
		return toSetVal() ? BooleanValue.falseInstance() : EMPTY_ARRAY_VALUE;
	}

	private boolean regexMatch(FieldValue str, FieldValue pattern) {
		if (!str.isString() || !pattern.isString()) {
			return false;
		}
		return Pattern.matches(pattern.getString(), str.getString());
	}

	// Let's illustrate with an example:
	// Say we have an object o = { x: 1, y: [ 1, 2, 3 ], z: [ 4, 5, 6, 7 ] },
	// and a path $.*[0:2] and want to perform a SET on that path
	// (e.g. valToSet = 10).
	// There are 2 segments DotSegment and BracketsSegment. First segment is
	// dot with wildcard. Since this is not the last segment, visiting wildcard
	// will yield an array [ o.x, o.y, o.z ]. For each element, we push it
	// on the stack and visit next segment (popping the element off the stack
	// afterward). The next segment is BracketsSegment with a slice selector.
	// This is the last segment, so we are ready to perform set. We visit it
	// 3 times for each of o.x, o.y, o.z. o.x is not an array, so no set is
	// performed and that visit returns false. For each of o.y and o.z, 2
	// elements will be set (with indexes 0 and 1), resulting in
	// o.y = [ 10, 10, 3 ] and o.z = [ 10, 10, 6, 7 ]. Each of these visits
	// returns true. Since at least one value was set, the overall result is
	// true.
	// If, in the last segment, instead of slice, we had a filter, e.g.
	// $.*[?(@.a.b.c < 10)], then when evaluating path @.a.b.c inside a filter,
	// we would return always return an array (and not boolean) despite being
	// in the last segment (usually it will be an array of 1 element for
	// comparison to make sense).
	private FieldValue visitSegments(ParserRuleContext segCtx,
        SegmentsContext otherCtx) {

		assert segCtx != null;
		if (!inFilter() && otherCtx == null) {
			inLastSegment = true;
		}

		FieldValue fldRes = visit(segCtx);
		// Note that even though filter expressions return a boolean, we will
		// not see it here, since we would only see the result of
		// visitFilterSelector().
		assert fldRes != null;
		assert toSetVal() ?	fldRes.isBoolean() : fldRes.isArray();

		if (!inFilter()) {
			inLastSegment = false;
		}

		if (otherCtx == null) {
			return fldRes;
		}

		// At this point we know that we are not in last segment and thus the
		// result of visiting of current segment (fldRes) should be an array.
		// Note that toSet is not the same as toSetVal(). Let's call it that
		// we are in "get" mode either when we are performing GET
		// (valToSet = null), or we are retrieving some path inside a filter.
		// Otherwise, we are in "set" mode. visitSegments will always return
		// an array in "get" mode and a boolean in "set" mode. The return type
		// of the last selector (in last segment) is maintained in the
		// recursive chain of calls visitSegments(), as done below.
		// On the other hand, the calls to visit selectors (see visit(segCtx)
		// above), will return boolean only if in "set" mode and in the last
		// segment. Otherwise, they will return an array and for each of its
		// elements we make the calls to visit segments down the tree (see loop
		// below).
		boolean toSet = (valToSet != null && !inFilter());
		FieldValue retVal = toSet ?
			BooleanValue.falseInstance() : new ArrayValue();

		for (FieldValue val : fldRes.asArray()) {
			stack.push(val);
			FieldValue res = visit(otherCtx);
			if (toSet) {
				assert res.isBoolean();
				if (res.getBoolean()) {
					retVal = BooleanValue.trueInstance();
				}
			} else {
				assert res.isArray();
				retVal.asArray().addAll(res.asArray().iterator());
			}
			stack.pop();
		}

		return retVal;
	}

	private FieldValue visitWildcard() {
		FieldValue currVal = stack.peek();
		assert currVal != null;

		if (currVal.isMap()) {
			MapValue mapVal = currVal.asMap();
			if (!toSetVal()) {
				return new ArrayValue().addAll(mapVal.values().iterator());
			}
			for(Map.Entry<String, FieldValue> entry : mapVal) {
				entry.setValue(copyValue(valToSet));
			}
			return BooleanValue.getInstance(mapVal.size() != 0);
		} else if (currVal.isArray()) {
			ArrayValue arrVal = currVal.asArray();
			if (!toSetVal()) {
				return arrVal;
			}
			for(int i = 0; i < arrVal.size(); i++) {
				arrVal.set(i, copyValue(valToSet));
			}
			return BooleanValue.getInstance(arrVal.size() != 0);
		} else {
			return emptyResult();
		}
	}

	// At this point, whether the aggregate is boolean or an array depends on
	// where we are in the tree, so we only ensure that aggr and res have the
	// same type. We use default null for nodes with no result and start
	// aggregation from first non-null result.
	protected FieldValue aggregateResult(FieldValue aggr, FieldValue res) {
		if (aggr == null) {
			return res;
		}
		if (res == null) {
			return aggr;
		}

		if (aggr.isBoolean()) {
			// Boolean results are aggregated with OR, since we return true
			// if at least one value has been set.
			assert res.isBoolean();
			return BooleanValue.getInstance(
				aggr.getBoolean() || res.getBoolean());
		}

		// Array results are aggregated by concatenating the arrays.
		assert aggr.isArray();
		assert res.isArray();
		return aggr.asArray().addAll(res.asArray().iterator());
	}

	public ArrayValue jsonGet(FieldValue rootVal, ParseTree parseTree,
		String input) {
		assert stack.isEmpty();
		assert inFilterCnt == 0;
		assert !inLastSegment;
		assert input != null;

		this.input = input;
		this.rootVal = rootVal;
		this.valToSet = null;

		FieldValue res = visit(parseTree);
		assert res.isArray();
		return res.asArray();
	}

	public boolean jsonSet(FieldValue rootVal, FieldValue valToSet,
		ParseTree parseTree, String input) {
		assert stack.isEmpty();
		assert inFilterCnt == 0;
		assert !inLastSegment;
		assert input != null;

		this.input = input;
		this.rootVal = rootVal;
		this.valToSet = valToSet;

		FieldValue res = visit(parseTree);
		assert res.isBoolean();
		return res.getBoolean();
	}

	@Override
	public FieldValue visitRootPath(JSONPathParser.RootPathContext ctx) {
		// We do not call this visitor for root path.
		assert ctx.segments() != null;
		assert rootVal != null;

		stack.push(rootVal);
		FieldValue ret = visitChildren(ctx);
		assert ret != null; // can only be null for root path
		stack.pop();

		return ret;
	}
	
	@Override
	public FieldValue visitAnyPath(JSONPathParser.AnyPathContext ctx) {
		// Currently this function is only called within filter, so the return
		// value will always be an array.
		assert inFilter();
		assert rootVal != null;
		assert !stack.isEmpty();

		boolean isRoot = ctx.DOLLAR() != null;
		if (isRoot) {
			stack.push(rootVal);
		}

		FieldValue ret = visitChildren(ctx);
		if (ret == null) {
			ret = new ArrayValue(1).add(stack.peek());
		}
		assert ret != null && ret.isArray();

		if (isRoot) {
			stack.pop();
		}
		return ret;
	}
	
	@Override
	public FieldValue visitJsonValue(JSONPathParser.JsonValueContext ctx) {
        try {
			// JSON values are used in filters on either side of a comparison
			// operator. On the other side, there will usually be some JSON
			// path (absolute with "$" or relative with "@"). Inside comparison
			// expression, we don't generally know which one we are evaluating.
			// Since evaluation of a path returns an array, we have to return
			// an array here also. The other way would be to use the fact that
			// if a comparison operand is either empty or has more than 1 value,
			// the result of comparison will always be false. But then we would
			// have to handle this filter case in each of the selectors that
			// may return multiple values. For simplicity, we keep result of
			// any visit... in "get" mode as an array (there is also a special
			// case of unary expression which would return true for operand
			// with multiple values, see visitComparisonExpr()).
			return new ArrayValue(1).add(
				FieldValue.createFromJson(ctx.getText(), null));
        } catch(JsonParseException ex) {
            throw parseException(ctx, "Invalid JSON value: " + ctx.getText(),
				ex);
        }
	}

	@Override
	public FieldValue visitDotSegment(JSONPathParser.DotSegmentContext ctx) {
		return visitSegments(ctx.field(), ctx.segments());
	}

	@Override
	public FieldValue visitBracketsSegment(
		JSONPathParser.BracketsSegmentContext ctx) {
		return visitSegments(ctx.brackets(), ctx.segments());
	}

	@Override
	public FieldValue visitDescendantSegment(
		JSONPathParser.DescendantSegmentContext ctx) {
		throw parseException(ctx, "Recursive descent is not supported");
	}

	@Override
	public FieldValue visitFieldId(JSONPathParser.FieldIdContext ctx) {
		FieldValue currVal = stack.peek();
		assert currVal != null;

		if (!currVal.isMap()) {
			return emptyResult();
		}

		String name = ctx.getText();

		if (toSetVal()) {
			currVal.asMap().put(name, copyValue(valToSet));
			return BooleanValue.trueInstance();
		}

		FieldValue fldVal = currVal.asMap().get(name);
		return fldVal != null ?
			new ArrayValue(1).add(fldVal) : EMPTY_ARRAY_VALUE;
	}
	
	@Override
	public FieldValue visitWildcard(JSONPathParser.WildcardContext ctx) {
		return visitWildcard();
	}

	@Override
	public FieldValue visitWildcardSelector(
		JSONPathParser.WildcardSelectorContext ctx) {
		return visitWildcard();
	}

	@Override
	public FieldValue visitFilterSelector(
		JSONPathParser.FilterSelectorContext ctx) {

		FieldValue currVal = stack.peek();
		assert currVal != null;

		boolean toSet = toSetVal();
		FieldValue retVal = toSet ?
			BooleanValue.falseInstance() : new ArrayValue();

		// For each value of the collection, we push it on the stack (for use
		// as "@" in filter) and evaluate the filter expression. If it passes
		// the filter, we aggregate it into the final result.

		if (currVal.isArray()) {
			ArrayValue arrVal = currVal.asArray();
			for (int i = 0; i < arrVal.size(); i++) {
				FieldValue val = arrVal.get(i);
				stack.push(val);
				FieldValue res = visit(ctx.filterExpr());
				assert res.isBoolean();
				if (res.getBoolean()) {
					if (toSet) {
						arrVal.set(i, copyValue(valToSet));
						retVal = BooleanValue.trueInstance();
					} else {
						retVal.asArray().add(val);
					}
				}
				stack.pop();
			}
		} else if (currVal.isMap()) {
			MapValue mapVal = currVal.asMap();
			for (Map.Entry<String, FieldValue> entry : mapVal) {
				FieldValue val = entry.getValue();
				stack.push(val);
				FieldValue res = visit(ctx.filterExpr());
				assert res.isBoolean();
				if (res.getBoolean()) {
					if (toSet) {
						entry.setValue(copyValue(valToSet));
						retVal = BooleanValue.trueInstance();
					} else {
						retVal.asArray().add(val);
					}
				}
				stack.pop();
			}
		}

		return retVal;
	}
	
	@Override
	public FieldValue visitIndexSelector(
		JSONPathParser.IndexSelectorContext ctx) {
		int idx;
		try {
			idx = Integer.parseInt(ctx.getText());
		} catch (NumberFormatException ex) {
			throw parseException(ctx, "Invalid array index: " +
				ctx.NUMBER().getText(), ex);
		}

		FieldValue currVal = stack.peek();
		assert currVal != null;
		if (!currVal.isArray()) {
			return emptyResult();
		}

		ArrayValue arrVal = currVal.asArray();
		int cnt = arrVal.size();
		if (idx < 0) {
			idx = cnt + idx;
		}
		if (idx < 0 || idx >= cnt) {
			return emptyResult();
		}

		if (toSetVal()) {
			arrVal.set(idx, copyValue(valToSet));
			return BooleanValue.trueInstance();
		}

		return new ArrayValue(1).add(arrVal.get(idx));
	}

	@Override
	public FieldValue visitSliceSelector(
		JSONPathParser.SliceSelectorContext ctx) {

		FieldValue currVal = stack.peek();
		assert currVal != null;
		if (!currVal.isArray()) {
			return emptyResult();
		}

		ArrayValue arrVal = currVal.asArray();
		int arrCnt = arrVal.size();
		int start = 0;
		int end = arrCnt;
		int step = 1;

		int nodeCnt = ctx.getChildCount();
		assert nodeCnt > 0;
		int argIdx = 0;
		for (int i = 0; i < nodeCnt; i++) {
			ParseTree node = ctx.getChild(i);
			assert node instanceof TerminalNode;
			if (((TerminalNode) node).getSymbol().getType() ==
				JSONPathParser.COLON) {
				argIdx++;
				continue;
			}

			assert ((TerminalNode) node).getSymbol().getType() ==
				JSONPathParser.NUMBER;
			int val;
			try {
				val = Integer.parseInt(node.getText());
			} catch (NumberFormatException ex) {
				throw parseException(ctx,
					"Invalid slice argument: " + node.getText(), ex);
			}

			switch (argIdx) {
				case 0:
					start = val >= 0 ? val : arrCnt - val;
					if (start < 0) {
						start = 0;
					}
					break;
				case 1:
					end = val >= 0 ? val : arrCnt - val;
					if (end > arrCnt) {
						end = arrCnt;
					}
					break;
				case 2:
					step = val;
					if (step <= 0) {
						throw parseException(ctx,
							"Slice step must be positive");
					}
					break;
				default:
					assert (false);
			}
		}

		if (toSetVal()) {
			for (int i = start; i < end; i += step) {
				arrVal.set(i, copyValue(valToSet));
			}
			// should work even if arrCnt = 0
			return BooleanValue.getInstance(start < end);
		}

		ArrayValue retVal = new ArrayValue();
		for (int i = start; i < end; i += step) {
			retVal.add(arrVal.get(i));
		}

		return retVal;
	}

	@Override
	public FieldValue visitMapSelector(JSONPathParser.MapSelectorContext ctx) {
		FieldValue currVal = stack.peek();
		assert currVal != null;
		if (!currVal.isMap()) {
			return emptyResult();
		}

		MapValue mapVal = currVal.asMap();
		String fldName = unquote(ctx.getText());

		if (toSetVal()) {
			mapVal.put(fldName, copyValue(valToSet));
			return BooleanValue.trueInstance();
		}

		FieldValue fldVal = currVal.asMap().get(fldName);
		return fldVal != null ?
			new ArrayValue(1).add(fldVal) : EMPTY_ARRAY_VALUE;
	}
	
	@Override
	public FieldValue visitFilterExpr(FilterExprContext ctx) {
		inFilterCnt++;

		AndExprContext andExpr = ctx.andExpr();
		assert andExpr != null;
		FieldValue andExprRes = visit(andExpr);
		assert andExprRes.isBoolean();
		FilterExprContext filterExpr = ctx.filterExpr();

		// true || any_val = true
		FieldValue retVal = filterExpr == null || andExprRes.getBoolean() ?
			andExprRes : visit(filterExpr);

		inFilterCnt--;
		return retVal;
	}
	
	@Override
	public FieldValue visitAndExpr(AndExprContext ctx) {
		BasicExprContext basicExpr = ctx.basicExpr();
		assert basicExpr != null;
		FieldValue basicExprRes = visit(basicExpr);
		assert basicExprRes.isBoolean();
		AndExprContext andExpr = ctx.andExpr();
		// false && any_val = false
		return andExpr == null || !basicExprRes.getBoolean() ?
			basicExprRes : visit(andExpr);
	}
	
	@Override
	public FieldValue visitParenExpr(JSONPathParser.ParenExprContext ctx) {
		FieldValue innerRes = visit(ctx.filterExpr());
		assert innerRes.isBoolean();
		return ctx.NOT() == null ?
			innerRes :
			BooleanValue.getInstance(!innerRes.getBoolean());
	}

	@Override
	public FieldValue visitComparisonExpr(
		JSONPathParser.ComparisonExprContext ctx) {
		List<PathOrValContext> vals = ctx.pathOrVal();
		int cnt = vals.size();

		FieldValue val0 = visit(vals.get(0));
		assert val0.isArray();

		if (cnt == 1) {
			assert ctx.comp() == null;
			// It seems that Redis JSON follows RFC 9535, which in this case
			// only test the existence of the item designated by the embedded
			// path rather than its Javascript boolean conversion, so the
			// values such as null, false, 0 and "" will pass the test.
			// It seems also that path resulting in multiple items evaluates
			// to true as well.
            return BooleanValue.getInstance(val0.asArray().size() != 0);
		}

		assert cnt == 2;
        FieldValue val1 = visit(vals.get(1));
		assert val1.isArray();

		// Binary comparison always results in false if one or more operands
		// has either no items or multiple items.
        if (val0.asArray().size() != 1 || val1.asArray().size() != 1) {
            return BooleanValue.falseInstance();
        }

		val0 = val0.asArray().get(0);
		val1 = val1.asArray().get(0);

        CompContext comp = ctx.comp();
		assert comp != null;
		assert comp.getChildCount() == 1;
		assert comp.getChild(0) instanceof TerminalNode;
        TerminalNode tComp = (TerminalNode)comp.getChild(0);
        boolean res = false;
        switch(tComp.getSymbol().getType()) {
			case JSONPathParser.EQ:
                res = filterEquals(val0, val1);
				break;
			case JSONPathParser.NE:
				res = !filterEquals(val0, val1);
				break;
			case JSONPathParser.LT:
				res = filterCanCompare(val0, val1) &&
					(filterCompare(val0, val1) < 0);
				break;
			case JSONPathParser.GT:
				res = filterCanCompare(val0, val1) &&
					(filterCompare(val0, val1) > 0);
				break;
			case JSONPathParser.LE:
				res = filterCanCompare(val0, val1) ?
					filterCompare(val0, val1) <= 0 : filterEquals(val0, val1);
				break;
			case JSONPathParser.GE:
				res = filterCanCompare(val0, val1) ?
					filterCompare(val0, val1) >= 0 : filterEquals(val0, val1);
				break;
			case JSONPathParser.MATCH:
                res = regexMatch(val0, val1);
			default:
				assert(false);
				break;
		}

		return BooleanValue.getInstance(res);
	}

}
