package streaming.core.datasource.impl

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import _root_.streaming.dsl.{ConnectMeta, DBMappingKey}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._
import org.apache.spark.sql.mlsql.session.MLSQLException

/**
  * Created by Lenovo on 2020/12/17.
  */
class MlsqlPhoenix(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLRegistry with WowParams {

  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "org.apache.phoenix.spark"

  override def shortFormat: String = "phoenix"

  override def dbSplitter: String = "."

  def toSplit = "\\."

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(toSplit, 2)
    } else {
      Array("", config.path)
    }
    var namespace = ""

    val format = config.config.getOrElse("implClass", shortFormat)    //phonix
    // 获取connect语法里的所有配置参数
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if (options.contains("namespace")) {
          namespace = options("namespace")
        }
        //reader.options(options)

        if (options.contains("zkUrl")){
          val zkUrl: String = options.getOrElse("zkUrl", "")
          println(zkUrl)
          reader.option("zkUrl",zkUrl)
        } else {
          new MLSQLException(s"${zkUrl.name} is error")
        }
      })
    }

    if (config.config.contains("namespace")) {
      namespace = config.config("namespace")
    }

    val inputTableName = if (namespace == "") _dbtable else s"${namespace}:${_dbtable}"   //table  USER
    println("+++++++++" + inputTableName)
    reader.option("table", inputTableName)

    //load configs should overwrite connect configs
    reader.options(config.config)
    val frame: DataFrame = reader.format(fullFormat).load()
    frame.show(false)
    frame
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = ???

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(toSplit, 2)
    } else {
      Array("", config.path)
    }

    var namespace = _dbname

    if (config.config.contains("namespace")) {
      namespace = config.config.get("namespace").get
    } else {
      if (_dbname != "") {
        val format = config.config.getOrElse("implClass", fullFormat)
        //获取connect语法里的信息
        ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
          if (options.contains("namespace")) {
            namespace = options.get("namespace").get
          }
        })
      }
    }

    SourceInfo(shortFormat, namespace, _dbtable)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  final val zkUrl: Param[String] = new Param[String](this, "zkUrl", "zk address")

}
