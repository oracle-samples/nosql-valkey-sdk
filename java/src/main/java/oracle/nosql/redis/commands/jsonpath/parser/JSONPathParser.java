// Generated from JSONPath.g4 by ANTLR 4.13.2
package oracle.nosql.redis.commands.jsonpath.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class JSONPathParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		DOLLAR=1, STAR=2, AT=3, DD=4, D=5, EQ=6, GE=7, GT=8, LE=9, LT=10, NE=11, 
		AND=12, OR=13, NOT=14, LC=15, RC=16, LB=17, RB=18, LP=19, RP=20, COLON=21, 
		COMMA=22, QM=23, TRUE=24, FALSE=25, NULL=26, MINUS=27, SQSTRING=28, ID=29, 
		STRING=30, NUMBER=31, WS=32;
	public static final int
		RULE_jsonpath = 0, RULE_pathOrVal = 1, RULE_segments = 2, RULE_field = 3, 
		RULE_brackets = 4, RULE_selectors = 5, RULE_arraySelector = 6, RULE_mapSelector = 7, 
		RULE_filterExpr = 8, RULE_andExpr = 9, RULE_basicExpr = 10, RULE_comp = 11, 
		RULE_json = 12, RULE_obj = 13, RULE_pair = 14, RULE_arr = 15, RULE_value = 16;
	private static String[] makeRuleNames() {
		return new String[] {
			"jsonpath", "pathOrVal", "segments", "field", "brackets", "selectors", 
			"arraySelector", "mapSelector", "filterExpr", "andExpr", "basicExpr", 
			"comp", "json", "obj", "pair", "arr", "value"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'$'", "'*'", "'@'", "'..'", "'.'", "'=='", "'>='", "'>'", "'<='", 
			"'<'", "'!='", "'&&'", "'||'", "'!'", "'{'", "'}'", "'['", "']'", "'('", 
			"')'", "':'", "','", "'?'", "'true'", "'false'", "'null'", "'-'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "DOLLAR", "STAR", "AT", "DD", "D", "EQ", "GE", "GT", "LE", "LT", 
			"NE", "AND", "OR", "NOT", "LC", "RC", "LB", "RB", "LP", "RP", "COLON", 
			"COMMA", "QM", "TRUE", "FALSE", "NULL", "MINUS", "SQSTRING", "ID", "STRING", 
			"NUMBER", "WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "JSONPath.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public JSONPathParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonpathContext extends ParserRuleContext {
		public JsonpathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonpath; }
	 
		public JsonpathContext() { }
		public void copyFrom(JsonpathContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RootPathContext extends JsonpathContext {
		public TerminalNode DOLLAR() { return getToken(JSONPathParser.DOLLAR, 0); }
		public TerminalNode EOF() { return getToken(JSONPathParser.EOF, 0); }
		public SegmentsContext segments() {
			return getRuleContext(SegmentsContext.class,0);
		}
		public RootPathContext(JsonpathContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitRootPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonpathContext jsonpath() throws RecognitionException {
		JsonpathContext _localctx = new JsonpathContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_jsonpath);
		int _la;
		try {
			_localctx = new RootPathContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(34);
			match(DOLLAR);
			setState(36);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 131120L) != 0)) {
				{
				setState(35);
				segments();
				}
			}

			setState(38);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathOrValContext extends ParserRuleContext {
		public PathOrValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathOrVal; }
	 
		public PathOrValContext() { }
		public void copyFrom(PathOrValContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnyPathContext extends PathOrValContext {
		public TerminalNode DOLLAR() { return getToken(JSONPathParser.DOLLAR, 0); }
		public TerminalNode AT() { return getToken(JSONPathParser.AT, 0); }
		public SegmentsContext segments() {
			return getRuleContext(SegmentsContext.class,0);
		}
		public AnyPathContext(PathOrValContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitAnyPath(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonValueContext extends PathOrValContext {
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public JsonValueContext(PathOrValContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitJsonValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathOrValContext pathOrVal() throws RecognitionException {
		PathOrValContext _localctx = new PathOrValContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_pathOrVal);
		int _la;
		try {
			setState(45);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DOLLAR:
			case AT:
				_localctx = new AnyPathContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(40);
				_la = _input.LA(1);
				if ( !(_la==DOLLAR || _la==AT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(42);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 131120L) != 0)) {
					{
					setState(41);
					segments();
					}
				}

				}
				break;
			case LC:
			case LB:
			case TRUE:
			case FALSE:
			case NULL:
			case STRING:
			case NUMBER:
				_localctx = new JsonValueContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(44);
				value();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SegmentsContext extends ParserRuleContext {
		public SegmentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_segments; }
	 
		public SegmentsContext() { }
		public void copyFrom(SegmentsContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescendantSegmentContext extends SegmentsContext {
		public TerminalNode DD() { return getToken(JSONPathParser.DD, 0); }
		public FieldContext field() {
			return getRuleContext(FieldContext.class,0);
		}
		public BracketsContext brackets() {
			return getRuleContext(BracketsContext.class,0);
		}
		public SegmentsContext segments() {
			return getRuleContext(SegmentsContext.class,0);
		}
		public DescendantSegmentContext(SegmentsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitDescendantSegment(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DotSegmentContext extends SegmentsContext {
		public TerminalNode D() { return getToken(JSONPathParser.D, 0); }
		public FieldContext field() {
			return getRuleContext(FieldContext.class,0);
		}
		public SegmentsContext segments() {
			return getRuleContext(SegmentsContext.class,0);
		}
		public DotSegmentContext(SegmentsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitDotSegment(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BracketsSegmentContext extends SegmentsContext {
		public BracketsContext brackets() {
			return getRuleContext(BracketsContext.class,0);
		}
		public SegmentsContext segments() {
			return getRuleContext(SegmentsContext.class,0);
		}
		public BracketsSegmentContext(SegmentsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitBracketsSegment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SegmentsContext segments() throws RecognitionException {
		SegmentsContext _localctx = new SegmentsContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_segments);
		int _la;
		try {
			setState(64);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case D:
				_localctx = new DotSegmentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(47);
				match(D);
				setState(48);
				field();
				setState(50);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 131120L) != 0)) {
					{
					setState(49);
					segments();
					}
				}

				}
				break;
			case LB:
				_localctx = new BracketsSegmentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(52);
				brackets();
				setState(54);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 131120L) != 0)) {
					{
					setState(53);
					segments();
					}
				}

				}
				break;
			case DD:
				_localctx = new DescendantSegmentContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(56);
				match(DD);
				setState(59);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STAR:
				case ID:
					{
					setState(57);
					field();
					}
					break;
				case LB:
					{
					setState(58);
					brackets();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(62);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 131120L) != 0)) {
					{
					setState(61);
					segments();
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FieldContext extends ParserRuleContext {
		public FieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field; }
	 
		public FieldContext() { }
		public void copyFrom(FieldContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WildcardContext extends FieldContext {
		public TerminalNode STAR() { return getToken(JSONPathParser.STAR, 0); }
		public WildcardContext(FieldContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitWildcard(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FieldIdContext extends FieldContext {
		public TerminalNode ID() { return getToken(JSONPathParser.ID, 0); }
		public FieldIdContext(FieldContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitFieldId(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FieldContext field() throws RecognitionException {
		FieldContext _localctx = new FieldContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_field);
		try {
			setState(68);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ID:
				_localctx = new FieldIdContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(66);
				match(ID);
				}
				break;
			case STAR:
				_localctx = new WildcardContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(67);
				match(STAR);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BracketsContext extends ParserRuleContext {
		public TerminalNode LB() { return getToken(JSONPathParser.LB, 0); }
		public SelectorsContext selectors() {
			return getRuleContext(SelectorsContext.class,0);
		}
		public TerminalNode RB() { return getToken(JSONPathParser.RB, 0); }
		public BracketsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_brackets; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitBrackets(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BracketsContext brackets() throws RecognitionException {
		BracketsContext _localctx = new BracketsContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_brackets);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(70);
			match(LB);
			setState(71);
			selectors();
			setState(72);
			match(RB);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectorsContext extends ParserRuleContext {
		public SelectorsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectors; }
	 
		public SelectorsContext() { }
		public void copyFrom(SelectorsContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WildcardSelectorContext extends SelectorsContext {
		public TerminalNode STAR() { return getToken(JSONPathParser.STAR, 0); }
		public WildcardSelectorContext(SelectorsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitWildcardSelector(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArraySelectorsContext extends SelectorsContext {
		public List<ArraySelectorContext> arraySelector() {
			return getRuleContexts(ArraySelectorContext.class);
		}
		public ArraySelectorContext arraySelector(int i) {
			return getRuleContext(ArraySelectorContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(JSONPathParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(JSONPathParser.COMMA, i);
		}
		public ArraySelectorsContext(SelectorsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitArraySelectors(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MapSelectorsContext extends SelectorsContext {
		public List<MapSelectorContext> mapSelector() {
			return getRuleContexts(MapSelectorContext.class);
		}
		public MapSelectorContext mapSelector(int i) {
			return getRuleContext(MapSelectorContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(JSONPathParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(JSONPathParser.COMMA, i);
		}
		public MapSelectorsContext(SelectorsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitMapSelectors(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FilterSelectorContext extends SelectorsContext {
		public TerminalNode QM() { return getToken(JSONPathParser.QM, 0); }
		public TerminalNode LP() { return getToken(JSONPathParser.LP, 0); }
		public FilterExprContext filterExpr() {
			return getRuleContext(FilterExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(JSONPathParser.RP, 0); }
		public FilterSelectorContext(SelectorsContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitFilterSelector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectorsContext selectors() throws RecognitionException {
		SelectorsContext _localctx = new SelectorsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_selectors);
		int _la;
		try {
			setState(96);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COLON:
			case NUMBER:
				_localctx = new ArraySelectorsContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(74);
				arraySelector();
				setState(79);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(75);
					match(COMMA);
					setState(76);
					arraySelector();
					}
					}
					setState(81);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case SQSTRING:
			case STRING:
				_localctx = new MapSelectorsContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(82);
				mapSelector();
				setState(87);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(83);
					match(COMMA);
					setState(84);
					mapSelector();
					}
					}
					setState(89);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case STAR:
				_localctx = new WildcardSelectorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(90);
				match(STAR);
				}
				break;
			case QM:
				_localctx = new FilterSelectorContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(91);
				match(QM);
				setState(92);
				match(LP);
				setState(93);
				filterExpr();
				setState(94);
				match(RP);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArraySelectorContext extends ParserRuleContext {
		public ArraySelectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arraySelector; }
	 
		public ArraySelectorContext() { }
		public void copyFrom(ArraySelectorContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SliceSelectorContext extends ArraySelectorContext {
		public List<TerminalNode> COLON() { return getTokens(JSONPathParser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(JSONPathParser.COLON, i);
		}
		public List<TerminalNode> NUMBER() { return getTokens(JSONPathParser.NUMBER); }
		public TerminalNode NUMBER(int i) {
			return getToken(JSONPathParser.NUMBER, i);
		}
		public SliceSelectorContext(ArraySelectorContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitSliceSelector(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IndexSelectorContext extends ArraySelectorContext {
		public TerminalNode NUMBER() { return getToken(JSONPathParser.NUMBER, 0); }
		public IndexSelectorContext(ArraySelectorContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitIndexSelector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArraySelectorContext arraySelector() throws RecognitionException {
		ArraySelectorContext _localctx = new ArraySelectorContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_arraySelector);
		int _la;
		try {
			setState(112);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
			case 1:
				_localctx = new IndexSelectorContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(98);
				match(NUMBER);
				}
				break;
			case 2:
				_localctx = new SliceSelectorContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(100);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NUMBER) {
					{
					setState(99);
					match(NUMBER);
					}
				}

				setState(102);
				match(COLON);
				setState(104);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NUMBER) {
					{
					setState(103);
					match(NUMBER);
					}
				}

				setState(110);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COLON) {
					{
					setState(106);
					match(COLON);
					setState(108);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==NUMBER) {
						{
						setState(107);
						match(NUMBER);
						}
					}

					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MapSelectorContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(JSONPathParser.STRING, 0); }
		public TerminalNode SQSTRING() { return getToken(JSONPathParser.SQSTRING, 0); }
		public MapSelectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mapSelector; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitMapSelector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MapSelectorContext mapSelector() throws RecognitionException {
		MapSelectorContext _localctx = new MapSelectorContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_mapSelector);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(114);
			_la = _input.LA(1);
			if ( !(_la==SQSTRING || _la==STRING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FilterExprContext extends ParserRuleContext {
		public AndExprContext andExpr() {
			return getRuleContext(AndExprContext.class,0);
		}
		public TerminalNode OR() { return getToken(JSONPathParser.OR, 0); }
		public FilterExprContext filterExpr() {
			return getRuleContext(FilterExprContext.class,0);
		}
		public FilterExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filterExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitFilterExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterExprContext filterExpr() throws RecognitionException {
		FilterExprContext _localctx = new FilterExprContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_filterExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(116);
			andExpr();
			setState(119);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OR) {
				{
				setState(117);
				match(OR);
				setState(118);
				filterExpr();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AndExprContext extends ParserRuleContext {
		public BasicExprContext basicExpr() {
			return getRuleContext(BasicExprContext.class,0);
		}
		public TerminalNode AND() { return getToken(JSONPathParser.AND, 0); }
		public AndExprContext andExpr() {
			return getRuleContext(AndExprContext.class,0);
		}
		public AndExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_andExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitAndExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AndExprContext andExpr() throws RecognitionException {
		AndExprContext _localctx = new AndExprContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_andExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(121);
			basicExpr();
			setState(124);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND) {
				{
				setState(122);
				match(AND);
				setState(123);
				andExpr();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BasicExprContext extends ParserRuleContext {
		public BasicExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_basicExpr; }
	 
		public BasicExprContext() { }
		public void copyFrom(BasicExprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonExprContext extends BasicExprContext {
		public List<PathOrValContext> pathOrVal() {
			return getRuleContexts(PathOrValContext.class);
		}
		public PathOrValContext pathOrVal(int i) {
			return getRuleContext(PathOrValContext.class,i);
		}
		public CompContext comp() {
			return getRuleContext(CompContext.class,0);
		}
		public ComparisonExprContext(BasicExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitComparisonExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenExprContext extends BasicExprContext {
		public TerminalNode LP() { return getToken(JSONPathParser.LP, 0); }
		public FilterExprContext filterExpr() {
			return getRuleContext(FilterExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(JSONPathParser.RP, 0); }
		public TerminalNode NOT() { return getToken(JSONPathParser.NOT, 0); }
		public ParenExprContext(BasicExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitParenExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BasicExprContext basicExpr() throws RecognitionException {
		BasicExprContext _localctx = new BasicExprContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_basicExpr);
		int _la;
		try {
			setState(139);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NOT:
			case LP:
				_localctx = new ParenExprContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(127);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(126);
					match(NOT);
					}
				}

				setState(129);
				match(LP);
				setState(130);
				filterExpr();
				setState(131);
				match(RP);
				}
				break;
			case DOLLAR:
			case AT:
			case LC:
			case LB:
			case TRUE:
			case FALSE:
			case NULL:
			case STRING:
			case NUMBER:
				_localctx = new ComparisonExprContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(133);
				pathOrVal();
				setState(137);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 4032L) != 0)) {
					{
					setState(134);
					comp();
					setState(135);
					pathOrVal();
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CompContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(JSONPathParser.EQ, 0); }
		public TerminalNode NE() { return getToken(JSONPathParser.NE, 0); }
		public TerminalNode LT() { return getToken(JSONPathParser.LT, 0); }
		public TerminalNode GT() { return getToken(JSONPathParser.GT, 0); }
		public TerminalNode LE() { return getToken(JSONPathParser.LE, 0); }
		public TerminalNode GE() { return getToken(JSONPathParser.GE, 0); }
		public CompContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitComp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CompContext comp() throws RecognitionException {
		CompContext _localctx = new CompContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_comp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(141);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 4032L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonContext extends ParserRuleContext {
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public TerminalNode EOF() { return getToken(JSONPathParser.EOF, 0); }
		public JsonContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitJson(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonContext json() throws RecognitionException {
		JsonContext _localctx = new JsonContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_json);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(143);
			value();
			setState(144);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObjContext extends ParserRuleContext {
		public TerminalNode LC() { return getToken(JSONPathParser.LC, 0); }
		public List<PairContext> pair() {
			return getRuleContexts(PairContext.class);
		}
		public PairContext pair(int i) {
			return getRuleContext(PairContext.class,i);
		}
		public TerminalNode RC() { return getToken(JSONPathParser.RC, 0); }
		public List<TerminalNode> COMMA() { return getTokens(JSONPathParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(JSONPathParser.COMMA, i);
		}
		public ObjContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_obj; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitObj(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ObjContext obj() throws RecognitionException {
		ObjContext _localctx = new ObjContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_obj);
		int _la;
		try {
			setState(159);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(146);
				match(LC);
				setState(147);
				pair();
				setState(152);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(148);
					match(COMMA);
					setState(149);
					pair();
					}
					}
					setState(154);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(155);
				match(RC);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(157);
				match(LC);
				setState(158);
				match(RC);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PairContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(JSONPathParser.STRING, 0); }
		public TerminalNode COLON() { return getToken(JSONPathParser.COLON, 0); }
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public PairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pair; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitPair(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PairContext pair() throws RecognitionException {
		PairContext _localctx = new PairContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_pair);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(161);
			match(STRING);
			setState(162);
			match(COLON);
			setState(163);
			value();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArrContext extends ParserRuleContext {
		public TerminalNode LB() { return getToken(JSONPathParser.LB, 0); }
		public List<ValueContext> value() {
			return getRuleContexts(ValueContext.class);
		}
		public ValueContext value(int i) {
			return getRuleContext(ValueContext.class,i);
		}
		public TerminalNode RB() { return getToken(JSONPathParser.RB, 0); }
		public List<TerminalNode> COMMA() { return getTokens(JSONPathParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(JSONPathParser.COMMA, i);
		}
		public ArrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitArr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArrContext arr() throws RecognitionException {
		ArrContext _localctx = new ArrContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_arr);
		int _la;
		try {
			setState(178);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(165);
				match(LB);
				setState(166);
				value();
				setState(171);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(167);
					match(COMMA);
					setState(168);
					value();
					}
					}
					setState(173);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(174);
				match(RB);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(176);
				match(LB);
				setState(177);
				match(RB);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(JSONPathParser.STRING, 0); }
		public TerminalNode NUMBER() { return getToken(JSONPathParser.NUMBER, 0); }
		public ObjContext obj() {
			return getRuleContext(ObjContext.class,0);
		}
		public ArrContext arr() {
			return getRuleContext(ArrContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(JSONPathParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(JSONPathParser.FALSE, 0); }
		public TerminalNode NULL() { return getToken(JSONPathParser.NULL, 0); }
		public ValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JSONPathVisitor ) return ((JSONPathVisitor<? extends T>)visitor).visitValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_value);
		try {
			setState(187);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(180);
				match(STRING);
				}
				break;
			case NUMBER:
				enterOuterAlt(_localctx, 2);
				{
				setState(181);
				match(NUMBER);
				}
				break;
			case LC:
				enterOuterAlt(_localctx, 3);
				{
				setState(182);
				obj();
				}
				break;
			case LB:
				enterOuterAlt(_localctx, 4);
				{
				setState(183);
				arr();
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 5);
				{
				setState(184);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 6);
				{
				setState(185);
				match(FALSE);
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 7);
				{
				setState(186);
				match(NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u0001 \u00be\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0001\u0000\u0001\u0000\u0003\u0000%\b\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0003\u0001+\b\u0001"+
		"\u0001\u0001\u0003\u0001.\b\u0001\u0001\u0002\u0001\u0002\u0001\u0002"+
		"\u0003\u00023\b\u0002\u0001\u0002\u0001\u0002\u0003\u00027\b\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0003\u0002<\b\u0002\u0001\u0002\u0003"+
		"\u0002?\b\u0002\u0003\u0002A\b\u0002\u0001\u0003\u0001\u0003\u0003\u0003"+
		"E\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0005\u0005N\b\u0005\n\u0005\f\u0005Q\t\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005V\b\u0005\n\u0005\f\u0005"+
		"Y\t\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005a\b\u0005\u0001\u0006\u0001\u0006\u0003\u0006"+
		"e\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006i\b\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006m\b\u0006\u0003\u0006o\b\u0006\u0003\u0006q\b\u0006"+
		"\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0003\bx\b\b\u0001\t"+
		"\u0001\t\u0001\t\u0003\t}\b\t\u0001\n\u0003\n\u0080\b\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n\u008a\b\n\u0003"+
		"\n\u008c\b\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001\r\u0001"+
		"\r\u0001\r\u0001\r\u0005\r\u0097\b\r\n\r\f\r\u009a\t\r\u0001\r\u0001\r"+
		"\u0001\r\u0001\r\u0003\r\u00a0\b\r\u0001\u000e\u0001\u000e\u0001\u000e"+
		"\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0005\u000f"+
		"\u00aa\b\u000f\n\u000f\f\u000f\u00ad\t\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0003\u000f\u00b3\b\u000f\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u00bc"+
		"\b\u0010\u0001\u0010\u0000\u0000\u0011\u0000\u0002\u0004\u0006\b\n\f\u000e"+
		"\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \u0000\u0003\u0002\u0000"+
		"\u0001\u0001\u0003\u0003\u0002\u0000\u001c\u001c\u001e\u001e\u0001\u0000"+
		"\u0006\u000b\u00cf\u0000\"\u0001\u0000\u0000\u0000\u0002-\u0001\u0000"+
		"\u0000\u0000\u0004@\u0001\u0000\u0000\u0000\u0006D\u0001\u0000\u0000\u0000"+
		"\bF\u0001\u0000\u0000\u0000\n`\u0001\u0000\u0000\u0000\fp\u0001\u0000"+
		"\u0000\u0000\u000er\u0001\u0000\u0000\u0000\u0010t\u0001\u0000\u0000\u0000"+
		"\u0012y\u0001\u0000\u0000\u0000\u0014\u008b\u0001\u0000\u0000\u0000\u0016"+
		"\u008d\u0001\u0000\u0000\u0000\u0018\u008f\u0001\u0000\u0000\u0000\u001a"+
		"\u009f\u0001\u0000\u0000\u0000\u001c\u00a1\u0001\u0000\u0000\u0000\u001e"+
		"\u00b2\u0001\u0000\u0000\u0000 \u00bb\u0001\u0000\u0000\u0000\"$\u0005"+
		"\u0001\u0000\u0000#%\u0003\u0004\u0002\u0000$#\u0001\u0000\u0000\u0000"+
		"$%\u0001\u0000\u0000\u0000%&\u0001\u0000\u0000\u0000&\'\u0005\u0000\u0000"+
		"\u0001\'\u0001\u0001\u0000\u0000\u0000(*\u0007\u0000\u0000\u0000)+\u0003"+
		"\u0004\u0002\u0000*)\u0001\u0000\u0000\u0000*+\u0001\u0000\u0000\u0000"+
		"+.\u0001\u0000\u0000\u0000,.\u0003 \u0010\u0000-(\u0001\u0000\u0000\u0000"+
		"-,\u0001\u0000\u0000\u0000.\u0003\u0001\u0000\u0000\u0000/0\u0005\u0005"+
		"\u0000\u000002\u0003\u0006\u0003\u000013\u0003\u0004\u0002\u000021\u0001"+
		"\u0000\u0000\u000023\u0001\u0000\u0000\u00003A\u0001\u0000\u0000\u0000"+
		"46\u0003\b\u0004\u000057\u0003\u0004\u0002\u000065\u0001\u0000\u0000\u0000"+
		"67\u0001\u0000\u0000\u00007A\u0001\u0000\u0000\u00008;\u0005\u0004\u0000"+
		"\u00009<\u0003\u0006\u0003\u0000:<\u0003\b\u0004\u0000;9\u0001\u0000\u0000"+
		"\u0000;:\u0001\u0000\u0000\u0000<>\u0001\u0000\u0000\u0000=?\u0003\u0004"+
		"\u0002\u0000>=\u0001\u0000\u0000\u0000>?\u0001\u0000\u0000\u0000?A\u0001"+
		"\u0000\u0000\u0000@/\u0001\u0000\u0000\u0000@4\u0001\u0000\u0000\u0000"+
		"@8\u0001\u0000\u0000\u0000A\u0005\u0001\u0000\u0000\u0000BE\u0005\u001d"+
		"\u0000\u0000CE\u0005\u0002\u0000\u0000DB\u0001\u0000\u0000\u0000DC\u0001"+
		"\u0000\u0000\u0000E\u0007\u0001\u0000\u0000\u0000FG\u0005\u0011\u0000"+
		"\u0000GH\u0003\n\u0005\u0000HI\u0005\u0012\u0000\u0000I\t\u0001\u0000"+
		"\u0000\u0000JO\u0003\f\u0006\u0000KL\u0005\u0016\u0000\u0000LN\u0003\f"+
		"\u0006\u0000MK\u0001\u0000\u0000\u0000NQ\u0001\u0000\u0000\u0000OM\u0001"+
		"\u0000\u0000\u0000OP\u0001\u0000\u0000\u0000Pa\u0001\u0000\u0000\u0000"+
		"QO\u0001\u0000\u0000\u0000RW\u0003\u000e\u0007\u0000ST\u0005\u0016\u0000"+
		"\u0000TV\u0003\u000e\u0007\u0000US\u0001\u0000\u0000\u0000VY\u0001\u0000"+
		"\u0000\u0000WU\u0001\u0000\u0000\u0000WX\u0001\u0000\u0000\u0000Xa\u0001"+
		"\u0000\u0000\u0000YW\u0001\u0000\u0000\u0000Za\u0005\u0002\u0000\u0000"+
		"[\\\u0005\u0017\u0000\u0000\\]\u0005\u0013\u0000\u0000]^\u0003\u0010\b"+
		"\u0000^_\u0005\u0014\u0000\u0000_a\u0001\u0000\u0000\u0000`J\u0001\u0000"+
		"\u0000\u0000`R\u0001\u0000\u0000\u0000`Z\u0001\u0000\u0000\u0000`[\u0001"+
		"\u0000\u0000\u0000a\u000b\u0001\u0000\u0000\u0000bq\u0005\u001f\u0000"+
		"\u0000ce\u0005\u001f\u0000\u0000dc\u0001\u0000\u0000\u0000de\u0001\u0000"+
		"\u0000\u0000ef\u0001\u0000\u0000\u0000fh\u0005\u0015\u0000\u0000gi\u0005"+
		"\u001f\u0000\u0000hg\u0001\u0000\u0000\u0000hi\u0001\u0000\u0000\u0000"+
		"in\u0001\u0000\u0000\u0000jl\u0005\u0015\u0000\u0000km\u0005\u001f\u0000"+
		"\u0000lk\u0001\u0000\u0000\u0000lm\u0001\u0000\u0000\u0000mo\u0001\u0000"+
		"\u0000\u0000nj\u0001\u0000\u0000\u0000no\u0001\u0000\u0000\u0000oq\u0001"+
		"\u0000\u0000\u0000pb\u0001\u0000\u0000\u0000pd\u0001\u0000\u0000\u0000"+
		"q\r\u0001\u0000\u0000\u0000rs\u0007\u0001\u0000\u0000s\u000f\u0001\u0000"+
		"\u0000\u0000tw\u0003\u0012\t\u0000uv\u0005\r\u0000\u0000vx\u0003\u0010"+
		"\b\u0000wu\u0001\u0000\u0000\u0000wx\u0001\u0000\u0000\u0000x\u0011\u0001"+
		"\u0000\u0000\u0000y|\u0003\u0014\n\u0000z{\u0005\f\u0000\u0000{}\u0003"+
		"\u0012\t\u0000|z\u0001\u0000\u0000\u0000|}\u0001\u0000\u0000\u0000}\u0013"+
		"\u0001\u0000\u0000\u0000~\u0080\u0005\u000e\u0000\u0000\u007f~\u0001\u0000"+
		"\u0000\u0000\u007f\u0080\u0001\u0000\u0000\u0000\u0080\u0081\u0001\u0000"+
		"\u0000\u0000\u0081\u0082\u0005\u0013\u0000\u0000\u0082\u0083\u0003\u0010"+
		"\b\u0000\u0083\u0084\u0005\u0014\u0000\u0000\u0084\u008c\u0001\u0000\u0000"+
		"\u0000\u0085\u0089\u0003\u0002\u0001\u0000\u0086\u0087\u0003\u0016\u000b"+
		"\u0000\u0087\u0088\u0003\u0002\u0001\u0000\u0088\u008a\u0001\u0000\u0000"+
		"\u0000\u0089\u0086\u0001\u0000\u0000\u0000\u0089\u008a\u0001\u0000\u0000"+
		"\u0000\u008a\u008c\u0001\u0000\u0000\u0000\u008b\u007f\u0001\u0000\u0000"+
		"\u0000\u008b\u0085\u0001\u0000\u0000\u0000\u008c\u0015\u0001\u0000\u0000"+
		"\u0000\u008d\u008e\u0007\u0002\u0000\u0000\u008e\u0017\u0001\u0000\u0000"+
		"\u0000\u008f\u0090\u0003 \u0010\u0000\u0090\u0091\u0005\u0000\u0000\u0001"+
		"\u0091\u0019\u0001\u0000\u0000\u0000\u0092\u0093\u0005\u000f\u0000\u0000"+
		"\u0093\u0098\u0003\u001c\u000e\u0000\u0094\u0095\u0005\u0016\u0000\u0000"+
		"\u0095\u0097\u0003\u001c\u000e\u0000\u0096\u0094\u0001\u0000\u0000\u0000"+
		"\u0097\u009a\u0001\u0000\u0000\u0000\u0098\u0096\u0001\u0000\u0000\u0000"+
		"\u0098\u0099\u0001\u0000\u0000\u0000\u0099\u009b\u0001\u0000\u0000\u0000"+
		"\u009a\u0098\u0001\u0000\u0000\u0000\u009b\u009c\u0005\u0010\u0000\u0000"+
		"\u009c\u00a0\u0001\u0000\u0000\u0000\u009d\u009e\u0005\u000f\u0000\u0000"+
		"\u009e\u00a0\u0005\u0010\u0000\u0000\u009f\u0092\u0001\u0000\u0000\u0000"+
		"\u009f\u009d\u0001\u0000\u0000\u0000\u00a0\u001b\u0001\u0000\u0000\u0000"+
		"\u00a1\u00a2\u0005\u001e\u0000\u0000\u00a2\u00a3\u0005\u0015\u0000\u0000"+
		"\u00a3\u00a4\u0003 \u0010\u0000\u00a4\u001d\u0001\u0000\u0000\u0000\u00a5"+
		"\u00a6\u0005\u0011\u0000\u0000\u00a6\u00ab\u0003 \u0010\u0000\u00a7\u00a8"+
		"\u0005\u0016\u0000\u0000\u00a8\u00aa\u0003 \u0010\u0000\u00a9\u00a7\u0001"+
		"\u0000\u0000\u0000\u00aa\u00ad\u0001\u0000\u0000\u0000\u00ab\u00a9\u0001"+
		"\u0000\u0000\u0000\u00ab\u00ac\u0001\u0000\u0000\u0000\u00ac\u00ae\u0001"+
		"\u0000\u0000\u0000\u00ad\u00ab\u0001\u0000\u0000\u0000\u00ae\u00af\u0005"+
		"\u0012\u0000\u0000\u00af\u00b3\u0001\u0000\u0000\u0000\u00b0\u00b1\u0005"+
		"\u0011\u0000\u0000\u00b1\u00b3\u0005\u0012\u0000\u0000\u00b2\u00a5\u0001"+
		"\u0000\u0000\u0000\u00b2\u00b0\u0001\u0000\u0000\u0000\u00b3\u001f\u0001"+
		"\u0000\u0000\u0000\u00b4\u00bc\u0005\u001e\u0000\u0000\u00b5\u00bc\u0005"+
		"\u001f\u0000\u0000\u00b6\u00bc\u0003\u001a\r\u0000\u00b7\u00bc\u0003\u001e"+
		"\u000f\u0000\u00b8\u00bc\u0005\u0018\u0000\u0000\u00b9\u00bc\u0005\u0019"+
		"\u0000\u0000\u00ba\u00bc\u0005\u001a\u0000\u0000\u00bb\u00b4\u0001\u0000"+
		"\u0000\u0000\u00bb\u00b5\u0001\u0000\u0000\u0000\u00bb\u00b6\u0001\u0000"+
		"\u0000\u0000\u00bb\u00b7\u0001\u0000\u0000\u0000\u00bb\u00b8\u0001\u0000"+
		"\u0000\u0000\u00bb\u00b9\u0001\u0000\u0000\u0000\u00bb\u00ba\u0001\u0000"+
		"\u0000\u0000\u00bc!\u0001\u0000\u0000\u0000\u001b$*-26;>@DOW`dhlnpw|\u007f"+
		"\u0089\u008b\u0098\u009f\u00ab\u00b2\u00bb";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}