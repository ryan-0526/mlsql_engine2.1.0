package tech.mlsql.autosuggest.funcs

import tech.mlsql.autosuggest.{DataType, FuncReg, MLSQLSQLFunction}

/**
  * Created by Lenovo on 2021/3/3.
  */
class Max extends FuncReg {
  override def register = {

  val func = MLSQLSQLFunction.apply("max").desc(Map(
  "zhDoc" ->
  """
    |max，获取最大值，可单独或者配合group by 使用
    |""".stripMargin,
  IS_AGG -> YES
  )).
  funcParam.
  param("max", DataType.STRING, false, Map("zhDoc" -> "列名或者常数",COLUMN->YES)).
  func.
  returnParam(DataType.NUMBER, true, Map(
  "zhDoc" ->
  """
    |long类型数字
    |""".stripMargin
  )).
  build
  func
}
}
