package au.com.cba.omnia.dataproducts.features

import org.scalacheck.Prop.forAll

import org.specs2._

import au.com.cba.omnia.maestro.api.{Field, Maestro}, Maestro.Fields

import Feature._, Value._
import FeatureMetadata.ValueType._

import Arbitraries._

import au.com.cba.omnia.dataproducts.features.test.thrift.Customer

/* More of an integration test based on a semi-realistic example. Individual feature components
 * are tested in PivotFeatureSpec that follows.
 */
object PivotFeatureSetSpec extends Specification with ScalaCheck { def is = s2"""
  PivotFeatureSet - Test an example set of features based on pivoting a record
  ===========
  An example feature set
    must generate expected metadata       $generateMetadata
    must generate expected feature values $generateFeatureValues

  Macro feature set
    must generate same metadata as test example $generateMetadataCompareMacro
    must generate same values as test example $generateFeatureValuesCompareMacro
"""

  import Type.{Categorical, Continuous}

  object CustomerFeatureSet extends PivotFeatureSet[Customer] {
    val namespace = "test.namespace"

    def entity(c: Customer) = c.id
    def time(c: Customer)   = c.time

    val name:   Feature[Customer, Str]      = pivot(Fields[Customer].Name,   Categorical)
    val age:    Feature[Customer, Integral] = pivot(Fields[Customer].Age,    Categorical)
    val height: Feature[Customer, Decimal]  = pivot(Fields[Customer].Height, Continuous)

    def features = List(name, age, height)
  }



  def generateMetadata = {
    val metadata = CustomerFeatureSet.generateMetadata

    metadata must_== List(
      FeatureMetadata[Str]     (CustomerFeatureSet.namespace, Fields[Customer].Name.name,   Categorical),
      FeatureMetadata[Integral](CustomerFeatureSet.namespace, Fields[Customer].Age.name,    Categorical),
      FeatureMetadata[Decimal] (CustomerFeatureSet.namespace, Fields[Customer].Height.name, Continuous)
    )
  }

  def generateMetadataCompareMacro = {
    val metadata = CustomerFeatureSet.generateMetadata
    val macroMetadata = PivotMacro.pivotThrift[Customer](CustomerFeatureSet.namespace, CustomerFeatureSet.entity, CustomerFeatureSet.time).features.map(_.metadata)



    macroMetadata must containAllOf(metadata.toSeq)
  }

  def generateFeatureValuesCompareMacro = forAll { (c:Customer) =>
    val values = CustomerFeatureSet.generate(c)
    val macroValues = PivotMacro.pivotThrift[Customer](CustomerFeatureSet.namespace, CustomerFeatureSet.entity, CustomerFeatureSet.time).generate(c)

    macroValues.map(it => (it.entity, it.value, it.time)) must containAllOf(values.toSeq.map(it => (it.entity, it.value, it.time)))
  }

  def generateFeatureValues = forAll { (c: Customer) => {
    val featureValues = CustomerFeatureSet.generate(c)

    featureValues must_== List(
      FeatureValue[Customer, Str]     (CustomerFeatureSet.name,   c.id, c.name,   c.time),
      FeatureValue[Customer, Integral](CustomerFeatureSet.age,    c.id, c.age,    c.time),
      FeatureValue[Customer, Decimal] (CustomerFeatureSet.height, c.id, c.height, c.time)
    )
  }}
}

object PivotFeatureSpec extends Specification with ScalaCheck { def is = s2"""
  Pivot Features - Test individual pivot feature components
  ===========
  Creating pivot feature metadata
    must pass namespace through         $metadataNamespace
    must use field name as feature name $metadataName
    must pass feature type through      $metadataFeatureType
    must derive value type from field   $metadataValueType

  Generating pivot feature values
    must pass underlying feature through $valueFeature
    must use specified id as entity      $valueEntity
    must use field's value as value      $valueValue
    must use specified time as time      $valueTime
"""

  def pivot(
    ns: Namespace,
    ft: Type,
    e:  Customer => EntityId,
    t:  Customer => Time,
    field: Field[Customer, _]) = {
    // Work around fact that Patterns.pivot requires field's value type,
    // which we don't get from fields arbitrary
    if (field == Fields[Customer].Name) {
      Patterns.pivot[Customer, Str, String](ns, ft, e, t, Fields[Customer].Name)
    } else if (field == Fields[Customer].Age) {
      Patterns.pivot[Customer, Integral, Int](ns, ft, e, t, Fields[Customer].Age)
    } else if (field == Fields[Customer].Height) {
      Patterns.pivot[Customer, Decimal, Double](ns, ft, e, t, Fields[Customer].Height)
    } else sys.error("Unknown field generated by arbitrary: " + field)
  }

  def metadataNamespace = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _]) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    feature.metadata.namespace must_== namespace
  }}

  def metadataName = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _]) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    feature.metadata.name must_== field.name
  }}

  def metadataFeatureType = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _]) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    feature.metadata.featureType must_== fType
  }}

  def metadataValueType = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _]) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    val expected = field match {
      case f if f == Fields[Customer].Name => StringType
      case f if f == Fields[Customer].Age => IntegralType
      case f if f == Fields[Customer].Height => DecimalType
    }
    feature.metadata.valueType must_== expected
  }}

  def valueFeature = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _], c: Customer) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    feature.generate(c) must beSome.like { case v => v.feature must_== feature }
  }}

  def valueEntity = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _], c: Customer) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    feature.generate(c) must beSome.like { case v => v.entity must_== c.id }
  }}

  def valueValue = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _], c: Customer) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    val expected = field match {
      case f if f == Fields[Customer].Name => Str(Option(c.name))
      case f if f == Fields[Customer].Age => Integral(Option(c.age))
      case f if f == Fields[Customer].Height => Decimal(Option(c.height))
    }
    feature.generate(c) must beSome.like { case v => v.value must_== expected }
  }}

  def valueTime = forAll { (namespace: Namespace, fType: Type, field: Field[Customer, _], c: Customer) => {
    val feature = pivot(namespace, fType, _.id, _.time, field)

    feature.generate(c) must beSome.like { case v => v.time must_== c.time }
  }}
}
