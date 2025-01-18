// Generated from JSONPath.g4 by ANTLR 4.13.2
package oracle.nosql.redis.commands.jsonpath.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link JSONPathParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface JSONPathVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by the {@code RootPath}
	 * labeled alternative in {@link JSONPathParser#jsonpath}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRootPath(JSONPathParser.RootPathContext ctx);
	/**
	 * Visit a parse tree produced by the {@code AnyPath}
	 * labeled alternative in {@link JSONPathParser#pathOrVal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnyPath(JSONPathParser.AnyPathContext ctx);
	/**
	 * Visit a parse tree produced by the {@code JsonValue}
	 * labeled alternative in {@link JSONPathParser#pathOrVal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJsonValue(JSONPathParser.JsonValueContext ctx);
	/**
	 * Visit a parse tree produced by the {@code DotSegment}
	 * labeled alternative in {@link JSONPathParser#segments}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDotSegment(JSONPathParser.DotSegmentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code BracketsSegment}
	 * labeled alternative in {@link JSONPathParser#segments}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBracketsSegment(JSONPathParser.BracketsSegmentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code DescendantSegment}
	 * labeled alternative in {@link JSONPathParser#segments}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescendantSegment(JSONPathParser.DescendantSegmentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code FieldId}
	 * labeled alternative in {@link JSONPathParser#field}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFieldId(JSONPathParser.FieldIdContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Wildcard}
	 * labeled alternative in {@link JSONPathParser#field}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcard(JSONPathParser.WildcardContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#brackets}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBrackets(JSONPathParser.BracketsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ArraySelectors}
	 * labeled alternative in {@link JSONPathParser#selectors}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArraySelectors(JSONPathParser.ArraySelectorsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code MapSelectors}
	 * labeled alternative in {@link JSONPathParser#selectors}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMapSelectors(JSONPathParser.MapSelectorsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code WildcardSelector}
	 * labeled alternative in {@link JSONPathParser#selectors}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcardSelector(JSONPathParser.WildcardSelectorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code FilterSelector}
	 * labeled alternative in {@link JSONPathParser#selectors}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFilterSelector(JSONPathParser.FilterSelectorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code IndexSelector}
	 * labeled alternative in {@link JSONPathParser#arraySelector}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIndexSelector(JSONPathParser.IndexSelectorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code SliceSelector}
	 * labeled alternative in {@link JSONPathParser#arraySelector}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSliceSelector(JSONPathParser.SliceSelectorContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#mapSelector}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMapSelector(JSONPathParser.MapSelectorContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#filterExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFilterExpr(JSONPathParser.FilterExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#andExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAndExpr(JSONPathParser.AndExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ParenExpr}
	 * labeled alternative in {@link JSONPathParser#basicExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenExpr(JSONPathParser.ParenExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ComparisonExpr}
	 * labeled alternative in {@link JSONPathParser#basicExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonExpr(JSONPathParser.ComparisonExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#comp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComp(JSONPathParser.CompContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#json}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJson(JSONPathParser.JsonContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#obj}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitObj(JSONPathParser.ObjContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#pair}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPair(JSONPathParser.PairContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#arr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArr(JSONPathParser.ArrContext ctx);
	/**
	 * Visit a parse tree produced by {@link JSONPathParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(JSONPathParser.ValueContext ctx);
}