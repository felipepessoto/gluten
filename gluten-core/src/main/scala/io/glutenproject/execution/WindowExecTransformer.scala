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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.WindowFunctionNode
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExecBase

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.SortField

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

case class WindowExecTransformer(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExecBase
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetricsUpdater(metrics)

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning(
        "No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (
      BackendsApiManager.getSettings.requiredChildOrderingForWindow()
      && GlutenConfig.getConf.veloxColumnarWindowType.equals("streaming")
    ) {
      // Velox StreamingWindow need to require child order.
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    } else {
      Seq(Nil)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  def genWindowParameters(): Any = {
    // Start with "WindowParameters:"
    val windowParametersStr = new StringBuffer("WindowParameters:")
    // isStreaming: 1 for streaming, 0 for sort
    val isStreaming: Int =
      if (GlutenConfig.getConf.veloxColumnarWindowType.equals("streaming")) 1 else 0

    windowParametersStr
      .append("isStreaming=")
      .append(isStreaming)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(windowParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  def getRelNode(
      context: SubstraitContext,
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      sortOrder: Seq[SortOrder],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // WindowFunction Expressions
    val windowExpressions = new JArrayList[WindowFunctionNode]()
    BackendsApiManager.getSparkPlanExecApiInstance.genWindowFunctionsNode(
      windowExpression,
      windowExpressions,
      originalInputAttributes,
      args
    )

    // Partition By Expressions
    val partitionsExpressions = partitionSpec
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, attributeSeq = child.output)
          .doTransform(args))
      .asJava

    // Sort By Expressions
    val sortFieldList =
      sortOrder.map {
        order =>
          val builder = SortField.newBuilder()
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
            .doTransform(args)
          builder.setExpr(exprNode.toProtobuf)

          (order.direction.sql, order.nullOrdering.sql) match {
            case ("ASC", "NULLS FIRST") =>
              builder.setDirectionValue(1);
            case ("ASC", "NULLS LAST") =>
              builder.setDirectionValue(2);
            case ("DESC", "NULLS FIRST") =>
              builder.setDirectionValue(3);
            case ("DESC", "NULLS LAST") =>
              builder.setDirectionValue(4);
            case _ =>
              builder.setDirectionValue(0);
          }
          builder.build()
      }.asJava
    if (!validation) {
      val extensionNode =
        ExtensionBuilder.makeAdvancedExtension(genWindowParameters(), null)
      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportWindowExec(windowExpression)) {
      return ValidationResult
        .notOk(s"Found unsupported window expression: ${windowExpression.mkString(", ")}")
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode = getRelNode(
      substraitContext,
      windowExpression,
      partitionSpec,
      orderSpec,
      child.output,
      operatorId,
      null,
      validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    if (windowExpression == null || windowExpression.isEmpty) {
      // The computing for this project is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel = getRelNode(
      context,
      windowExpression,
      partitionSpec,
      orderSpec,
      child.output,
      operatorId,
      childCtx.root,
      validation = false)
    assert(currRel != null, "Window Rel should be valid")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowExecTransformer =
    copy(child = newChild)
}

object WindowExecTransformer {

  /** Gets lower/upper bound represented in string. */
  def getFrameBound(bound: Expression): String = {
    // The lower/upper can be either a foldable Expression or a SpecialFrameBoundary.
    if (bound.foldable) {
      bound.eval().toString
    } else {
      bound.sql
    }
  }
}
