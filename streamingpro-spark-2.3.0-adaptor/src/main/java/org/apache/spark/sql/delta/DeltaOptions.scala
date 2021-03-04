/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.util.regex.PatternSyntaxException

import scala.util.Try
import scala.util.matching.Regex

import org.apache.spark.sql.delta.DeltaOptions.{MERGE_SCHEMA_OPTION, OVERWRITE_SCHEMA_OPTION}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf


trait DeltaOptionParser {
  protected def sqlConf: SQLConf
  protected def options: CaseInsensitiveMap[String]

  def toBoolean(input: String, name: String): Boolean = {
    Try(input.toBoolean).toOption.getOrElse {
      throw DeltaErrors.illegalDeltaOptionException(name, input, "must be 'true' or 'false'")
    }
  }
}

trait DeltaWriteOptions
  extends DeltaWriteOptionsImpl

    with DeltaOptionParser {

  import DeltaOptions._

  val replaceWhere: Option[String] = options.get(REPLACE_WHERE_OPTION)

  /**
    * Whether to add an adaptive shuffle before writing out the files to break skew, and coalesce
    * data into chunkier files.
    */
  val optimizeWrite: Option[Boolean] = options.get(OPTIMIZE_WRITE_OPTION)
    .map(toBoolean(_, OPTIMIZE_WRITE_OPTION))
}

trait DeltaWriteOptionsImpl extends DeltaOptionParser {
  /**
    * Whether the user has enabled auto schema merging in writes using either a DataFrame option
    * or SQL Session configuration. Automerging is off when table ACLs are enabled.
    * We always respect the DataFrame writer configuration over the session config.
    */
  def canMergeSchema: Boolean = {
    options.get(MERGE_SCHEMA_OPTION)
      .map(toBoolean(_, MERGE_SCHEMA_OPTION))
      .getOrElse(sqlConf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE))
  }

  /**
    * Whether to allow overwriting the schema of a Delta table in an overwrite mode operation. If
    * ACLs are enabled, we can't change the schema of an operation through a write, which requires
    * MODIFY permissions, when schema changes require OWN permissions.
    */
  def canOverwriteSchema: Boolean = {
    options.get(OVERWRITE_SCHEMA_OPTION).map(toBoolean(_, OVERWRITE_SCHEMA_OPTION)).getOrElse(false)
  }
}

trait DeltaReadOptions extends DeltaOptionParser {
  import DeltaOptions._

  val maxFilesPerTrigger = options.get(MAX_FILES_PER_TRIGGER_OPTION).map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw DeltaErrors.illegalDeltaOptionException(
        MAX_FILES_PER_TRIGGER_OPTION, str, "must be a positive integer")
    }
  }

  val ignoreFileDeletion = options.get(IGNORE_FILE_DELETION_OPTION)
    .map(toBoolean(_, IGNORE_FILE_DELETION_OPTION)).getOrElse(false)

  val ignoreChanges = options.get(IGNORE_CHANGES_OPTION)
    .map(toBoolean(_, IGNORE_CHANGES_OPTION)).getOrElse(false)

  val ignoreDeletes = options.get(IGNORE_DELETES_OPTION)
    .map(toBoolean(_, IGNORE_DELETES_OPTION)).getOrElse(false)

  val excludeRegex: Option[Regex] = try options.get(EXCLUDE_REGEX_OPTION).map(_.r) catch {
    case e: PatternSyntaxException =>
      throw new IllegalArgumentException(
        s"Please recheck your syntax for '$EXCLUDE_REGEX_OPTION'", e)
  }
}


/**
  * Options for the Delta data source.
  */
class DeltaOptions(
                    @transient protected val options: CaseInsensitiveMap[String],
                    @transient protected val sqlConf: SQLConf)
  extends DeltaWriteOptions with DeltaReadOptions with Serializable {

  def this(options: Map[String, String], conf: SQLConf) = this(CaseInsensitiveMap(options), conf)
}

object DeltaOptions {

  /** An option to overwrite only the data that matches predicates over partition columns. */
  val REPLACE_WHERE_OPTION = "replaceWhere"
  /** An option to allow automatic schema merging during a write operation. */
  val MERGE_SCHEMA_OPTION = "mergeSchema"
  /** An option to allow overwriting schema and partitioning during an overwrite write operation. */
  val OVERWRITE_SCHEMA_OPTION = "overwriteSchema"

  val MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger"
  val MAX_FILES_PER_TRIGGER_OPTION_DEFAULT = 1000
  val EXCLUDE_REGEX_OPTION = "excludeRegex"
  val IGNORE_FILE_DELETION_OPTION = "ignoreFileDeletion"
  val IGNORE_CHANGES_OPTION = "ignoreChanges"
  val IGNORE_DELETES_OPTION = "ignoreDeletes"
  val OPTIMIZE_WRITE_OPTION = "optimizeWrite"
}
