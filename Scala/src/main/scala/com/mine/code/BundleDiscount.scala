package com.mine.code

object BundleDiscount {
  def main(args: Array[String]): Unit = {

    println(price(Book("《本草纲目》", 50)))

    println(price(Bundle("联合打折", 20, Book("《背影》", 66))))

    def price(item: Item): Double = item match {
      case Book(_, p) => p
      case Bundle(_, discount, item@_*) => item.map(price).sum - discount
    }
  }

  sealed abstract class Item

  case class Book(description: String, price: Double) extends Item

  case class Bundle(description: String, discount: Double, item: Item*) extends Item

}
