/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.sona.ml.feature

import org.apache.spark.linalg.VectorUDT

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers
import org.apache.spark.sql.types._

/**
  * Represents a parsed R formula.
  */
private[sona] case class ParsedRFormula(label: ColumnRef, terms: Seq[Term]) {
  /**
    * Resolves formula terms into column names. A schema is necessary for inferring the meaning
    * of the special '.' term. Duplicate terms will be removed during resolution.
    */
  def resolve(schema: StructType): ResolvedRFormula = {
    val dotTerms = expandDot(schema)
    var includedTerms = Seq[Seq[String]]()
    terms.foreach {
      case col: ColumnRef =>
        includedTerms :+= Seq(col.value)
      case ColumnInteraction(cols) =>
        includedTerms ++= expandInteraction(schema, cols)
      case Dot =>
        includedTerms ++= dotTerms.map(Seq(_))
      case Deletion(term: Term) =>
        term match {
          case inner: ColumnRef =>
            includedTerms = includedTerms.filter(_ != Seq(inner.value))
          case ColumnInteraction(cols) =>
            val fromInteraction = expandInteraction(schema, cols).map(_.toSet)
            includedTerms = includedTerms.filter(t => !fromInteraction.contains(t.toSet))
          case Dot =>
            // e.g. "- .", which removes all first-order terms
            includedTerms = includedTerms.filter {
              case Seq(t) => !dotTerms.contains(t)
              case _ => true
            }
          case _: Deletion =>
            throw new RuntimeException("Deletion terms cannot be nested")
          case _: Intercept =>
        }
      case _: Intercept =>
    }
    ResolvedRFormula(label.value, includedTerms.distinct, hasIntercept)
  }

  /** Whether this formula specifies fitting with response variable. */
  def hasLabel: Boolean = label.value.nonEmpty

  /** Whether this formula specifies fitting with an intercept term. */
  def hasIntercept: Boolean = {
    var intercept = true
    terms.foreach {
      case Intercept(enabled) =>
        intercept = enabled
      case Deletion(Intercept(enabled)) =>
        intercept = !enabled
      case _ =>
    }
    intercept
  }

  // expands the Dot operators in interaction terms
  private def expandInteraction(schema: StructType, terms: Seq[InteractableTerm]): Seq[Seq[String]] = {
    if (terms.isEmpty) {
      return Seq(Nil)
    }

    val rest = expandInteraction(schema, terms.tail)
    val validInteractions = (terms.head match {
      case Dot =>
        expandDot(schema).flatMap { t =>
          rest.map { r =>
            Seq(t) ++ r
          }
        }
      case ColumnRef(value) =>
        rest.map(Seq(value) ++ _)
    }).map(_.distinct)

    // Deduplicates feature interactions, for example, a:b is the same as b:a.
    val seen = mutable.Set[Set[String]]()
    validInteractions.flatMap {
      case t if seen.contains(t.toSet) =>
        None
      case t =>
        seen += t.toSet
        Some(t)
    }.sortBy(_.length)
  }

  // the dot operator excludes complex column types
  private def expandDot(schema: StructType): Seq[String] = {
    schema.fields.filter(_.dataType match {
      case _: NumericType | StringType | BooleanType | _: VectorUDT => true
      case _ => false
    }).map(_.name).filter(_ != label.value)
  }
}

/**
  * Represents a fully evaluated and simplified R formula.
  *
  * @param label        the column name of the R formula label (response variable).
  * @param terms        the simplified terms of the R formula. Interactions terms are represented as Seqs
  *                     of column names; non-interaction terms as length 1 Seqs.
  * @param hasIntercept whether the formula specifies fitting with an intercept.
  */
private[sona] case class ResolvedRFormula(label: String, terms: Seq[Seq[String]], hasIntercept: Boolean) {
  override def toString: String = {
    val ts = terms.map {
      case t if t.length > 1 =>
        s"${t.mkString("{", ",", "}")}"
      case t =>
        t.mkString
    }
    val termStr = ts.mkString("[", ",", "]")
    s"ResolvedRFormula(label=$label, terms=$termStr, hasIntercept=$hasIntercept)"
  }
}

/**
  * R formula terms. See the R formula docs here for more information:
  * http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html
  */
private[sona] sealed trait Term

/** A term that may be part of an interaction, e.g. 'x' in 'x:y' */
private[sona] sealed trait InteractableTerm extends Term

/* R formula reference to all available columns, e.g. "." in a formula */
private[sona] case object Dot extends InteractableTerm

/* R formula reference to a column, e.g. "+ Species" in a formula */
private[sona] case class ColumnRef(value: String) extends InteractableTerm

/* R formula interaction of several columns, e.g. "Sepal_Length:Species" in a formula */
private[sona] case class ColumnInteraction(terms: Seq[InteractableTerm]) extends Term

/* R formula intercept toggle, e.g. "+ 0" in a formula */
private[sona] case class Intercept(enabled: Boolean) extends Term

/* R formula deletion of a variable, e.g. "- Species" in a formula */
private[sona] case class Deletion(term: Term) extends Term

/**
  * Limited implementation of R formula parsing. Currently supports: '~', '+', '-', '.', ':'.
  */
private[sona] object RFormulaParser extends RegexParsers {
  private val intercept: Parser[Intercept] =
    "([01])".r ^^ { case a => Intercept(a == "1") }

  private val columnRef: Parser[ColumnRef] =
    "([a-zA-Z]|\\.[a-zA-Z_])[a-zA-Z0-9._]*".r ^^ { case a => ColumnRef(a) }

  private val empty: Parser[ColumnRef] = "" ^^ { case a => ColumnRef("") }

  private val label: Parser[ColumnRef] = columnRef | empty

  private val dot: Parser[InteractableTerm] = "\\.".r ^^ { case _ => Dot }

  private val interaction: Parser[List[InteractableTerm]] = rep1sep(columnRef | dot, ":")

  private val term: Parser[Term] = intercept |
    interaction ^^ { case terms => ColumnInteraction(terms) } | dot | columnRef

  private val terms: Parser[List[Term]] = (term ~ rep("+" ~ term | "-" ~ term)) ^^ {
    case op ~ list => list.foldLeft(List(op)) {
      case (left, "+" ~ right) => left ++ Seq(right)
      case (left, "-" ~ right) => left ++ Seq(Deletion(right))
    }
  }

  private val formula: Parser[ParsedRFormula] =
    (label ~ "~" ~ terms) ^^ { case r ~ "~" ~ t => ParsedRFormula(r, t) }

  def parse(value: String): ParsedRFormula = parseAll(formula, value) match {
    case Success(result, _) => result
    case failure: NoSuccess => throw new IllegalArgumentException(
      "Could not parse formula: " + value)
  }
}
