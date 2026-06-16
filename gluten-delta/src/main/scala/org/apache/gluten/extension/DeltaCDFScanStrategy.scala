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
package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class DeltaCDFScanStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, relation: LogicalRelation) =>
      relation.relation match {
        case cdfRelation: CDCReader.DeltaCDFRelation if !touchesDeletionVectors(cdfRelation) =>
          planCDFRelation(relation, cdfRelation, projects, filters).map(planLater).toSeq
        case _ => Nil
      }
    case _ => Nil
  }

  // Delta CDF over a deletion-vector table needs Delta's DV-aware, row-level reconciliation: a
  // deleted row stays physically in its data file and is masked by a DV, so the CDF `delete` rows
  // for a commit are only the rows the DV newly marks. Re-planning the analyzed CDF batch plan here
  // loses that reconciliation on some Delta versions (e.g. Delta 2.4 / Spark 3.4), where the remove
  // side would then surface every row of a logically-removed file as a `delete` change row instead
  // of just the DV-marked ones. Decline to intercept such reads and let Spark serve them through
  // Delta's own DeltaCDFRelation scan, which applies DVs correctly. Offloading loses nothing here:
  // the native CDF scan path does not support DV reconciliation either (see the matching guard in
  // DeltaScanTransformer).
  private def touchesDeletionVectors(cdfRelation: CDCReader.DeltaCDFRelation): Boolean =
    cdfRelation.snapshotWithSchemaMode.snapshot.metadata.configuration
      .get("delta.enableDeletionVectors")
      .exists(_.equalsIgnoreCase("true"))

  private def planCDFRelation(
      relation: LogicalRelation,
      cdfRelation: CDCReader.DeltaCDFRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): Option[LogicalPlan] = {
    if (cdfRelation.startingVersion.isEmpty) {
      return Some(projectAndFilter(LocalRelation(relation.output), projects, filters))
    }

    val cdfPlan =
      DeltaCDFRelationHelper.changesToBatchDF(cdfRelation, spark).queryExecution.analyzed
    val cdfOutput = cdfPlan.output
    val rewrittenFilters = filters.map(rewriteExpression(_, cdfOutput))
    val rewrittenProjects = projects.map(rewriteProject(_, cdfOutput))
    Some(projectAndFilter(cdfPlan, rewrittenProjects, rewrittenFilters))
  }

  private def projectAndFilter(
      child: LogicalPlan,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): LogicalPlan = {
    val filtered = filters.reduceOption(org.apache.spark.sql.catalyst.expressions.And) match {
      case Some(condition) => Filter(condition, child)
      case None => child
    }
    Project(projects, filtered)
  }

  private def rewriteProject(
      project: NamedExpression,
      cdfOutput: Seq[Attribute]): NamedExpression = {
    project match {
      case attr: AttributeReference =>
        Alias(
          resolveCDFAttribute(attr, cdfOutput),
          attr.name)(
          exprId = attr.exprId,
          qualifier = attr.qualifier,
          explicitMetadata = Some(attr.metadata))
      case other =>
        rewriteExpression(other, cdfOutput).asInstanceOf[NamedExpression]
    }
  }

  private def rewriteExpression(expr: Expression, cdfOutput: Seq[Attribute]): Expression = {
    expr.transform {
      case attr: AttributeReference => resolveCDFAttribute(attr, cdfOutput)
    }
  }

  private def resolveCDFAttribute(
      attr: AttributeReference,
      cdfOutput: Seq[Attribute]): Attribute = {
    cdfOutput
      .find(output => spark.sessionState.conf.resolver(output.name, attr.name))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unable to resolve CDF attribute ${attr.name} from " +
            s"${cdfOutput.map(_.name).mkString("[", ", ", "]")}"))
  }
}
