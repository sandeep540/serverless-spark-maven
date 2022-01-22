import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object MainApp extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("serverless-spark")
    .getOrCreate()


  import spark.implicits._

  val sku1 = new Sku(10, "red", "RED")
  val sku2 = new Sku(20, "blue", "BLUE")

  val product1 = new Product(101, "shirt", "Acme", Array(sku1))
  val product2 = new Product(101, "trouser", "Acme", Array(sku2))
  val product3 = new Product(102, "vest", "Acme", Array(sku1, sku2))

  val skufamily: Dataset[Sku] = Seq(sku1, sku2).toDS()

  skufamily.show()

  val productfamily: Dataset[Product] = Seq(product1, product2, product3).toDS()

  productfamily.show()

}

final case class Product (
                           id: Int,
                           name: String,
                           brand: String,
                           sku: Array[Sku]
                         )


final case class Sku (
                       id: Int,
                       name: String,
                       color: String
                     )
