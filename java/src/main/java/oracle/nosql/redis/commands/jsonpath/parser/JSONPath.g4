grammar JSONPath;
import JSON;

DOLLAR : '$' ;
STAR : '*' ;
AT : '@' ;
DD : '..' ;
D : '.' ;
EQ : '==' ;
GE : '>=' ;
GT : '>' ;
LE : '<=' ;
LT : '<' ;
NE : '!=' ;
AND : '&&' ;
OR : '||' ;
NOT : '!' ;
LC : '{' ;
RC : '}' ;
LB : '[' ;
RB : ']' ;
LP : '(' ;
RP : ')' ;
COLON : ':' ;
COMMA : ',' ;
QM : '?' ;
TRUE : 'true' ;
FALSE : 'false' ;
NULL : 'null' ;
MINUS : '-' ;
MATCH : '=~' ;

SQSTRING
    : '\'' (ESC | SAFECODEPOINT)* '\''
    ;

ID
   : [_A-Za-z] [_A-Za-z0-9]*
   ;

jsonpath
   : DOLLAR segments? EOF                 # RootPath
   ;

pathOrVal
   : ( DOLLAR | AT ) segments?            # AnyPath
   | value                                # JsonValue
   ;

segments
   : D field segments?                    # DotSegment
   | brackets segments?                   # BracketsSegment
   | DD ( field | brackets ) segments?    # DescendantSegment
   ;

field
   : ID                                   # FieldId
   | STAR                                 # Wildcard
   ;

brackets
   : LB selectors RB
   ;

selectors
   : arraySelector ( COMMA arraySelector )* # ArraySelectors
   | mapSelector ( COMMA mapSelector )*     # MapSelectors
   | STAR                                   # WildcardSelector
   | QM LP filterExpr RP                    # FilterSelector
   ;

arraySelector
   : NUMBER                                     # IndexSelector
   | NUMBER? COLON NUMBER? ( COLON NUMBER? )?   # SliceSelector
   ;

mapSelector
   : STRING | SQSTRING
   ;

filterExpr
   : andExpr ( OR filterExpr )?
   ;

andExpr
   : basicExpr ( AND andExpr )?
   ;

basicExpr
   : NOT? LP filterExpr RP                    # ParenExpr
   | pathOrVal ( comp pathOrVal )?            # ComparisonExpr
   ;

comp
   : EQ | NE | LT | GT | LE | GE | MATCH
   ;
