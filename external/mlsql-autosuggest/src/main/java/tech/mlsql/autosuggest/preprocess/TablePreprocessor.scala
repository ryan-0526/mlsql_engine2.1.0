package tech.mlsql.autosuggest.preprocess

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.SpecialTableConst.TEMP_TABLE_DB_KEY
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.statement.{PreProcessStatement, SelectSuggester}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TablePreprocessor(context: AutoSuggestContext) extends PreProcessStatement {

  def cleanStr(str: String) = {
    if (str.startsWith("`") || str.startsWith("\"") || (str.startsWith("'") && !str.startsWith("'''")))
      str.substring(1, str.length - 1)
    else str
  }

  /**
   * //提取load和select语句中表属性名
   * load 语句和select语句比较特殊
   * Load语句要获取 真实表
   * select 语句要获取最后的select 语句
   *
   * load语句获取真实表的时候会加一个prefix前缀，该值等于load语句里的format
   */
  def process(statement: List[Token]): Unit = {
    val tempTableProvider = context.tempTableProvider   //获取存储表名对象
    val tempMatcher = TokenMatcher(statement, statement.size - 2).back.eat(Food(None, DSLSQLLexer.IDENTIFIER), Food(None, DSLSQLLexer.AS)).build  //判断语句中是否存在表名(as tableName)

    if (tempMatcher.isSuccess) {
      val tableName = tempMatcher.getMatchTokens.last.getText
      val defaultTable = SpecialTableConst.tempTable(tableName) //默认表信息MetaTable
      val table = statement(0).getText.toLowerCase match {
        case "load" =>
          val formatMatcher = TokenMatcher(statement, 1).
            eat(Food(None, DSLSQLLexer.IDENTIFIER),
              Food(None, MLSQLTokenTypeWrapper.DOT),
              Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).build
          if (formatMatcher.isSuccess) {

            formatMatcher.getMatchTokens.map(_.getText) match {
              case List(format, _, path) =>
                cleanStr(path).split("\\.", 2) match {
                  case Array(db, table) =>
//                    if(context.isSchemaInferEnabled){
//
//                    }
                    context.metaProvider.search(MetaTableKey(Option(format), Option(db), table)).getOrElse(defaultTable)
                  case Array(table) =>
                    context.metaProvider.search(MetaTableKey(Option(format), None, table)).getOrElse(defaultTable)
                }
            }
          } else {
            defaultTable
          }
        case "select" =>
          //statement.size - 3 是为了移除 最后的as table;语句,为了防止将表名添加到列存储中，， slice语句跟subString方法一样，截前不截后
          val selectSuggester = new SelectSuggester(context, statement.slice(0, statement.size - 3), TokenPos(0, TokenPosType.NEXT, -1))
          val columns = selectSuggester.sqlAST.output(selectSuggester.tokens).map { name =>
            MetaTableColumn(name, null, true, Map())
          }
          MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), tableName), columns)
        case _ => defaultTable
      }

      tempTableProvider.register(tableName, table)
    }
  }
}
