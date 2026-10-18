package fraud

import scala.util.Random

final case class Transaction(
    transactionId: String,
    cardId: String,
    merchantId: String,
    category: String,
    country: String,
    ts: Long,
    amount: Double,
    isFraud: Boolean
)

object TransactionGenerator {

  private val Categories = Vector("grocery", "fuel", "restaurant", "online", "travel", "electronics", "pharmacy")
  private val HomeCountry = "FR"
  private val ForeignCountries = Vector("US", "GB", "DE", "ES", "IT", "NG", "RU", "CN")
  private val StartEpoch = 1735689600L // 2025-01-01T00:00:00Z
  private val SecondsPerDay = 86400L
  private val Days = 30

  /** Génère `n` transactions de façon déterministe : même graine, même résultat.
    * Le nombre de fraudes vaut exactement round(n * fraudRate).
    */
  def generate(n: Int, seed: Long, fraudRate: Double): Vector[Transaction] = {
    require(n >= 0, "n doit être positif")
    require(fraudRate >= 0.0 && fraudRate <= 1.0, "fraudRate doit être dans [0, 1]")

    val rnd = new Random(seed)
    val fraudCount = math.round(n * fraudRate).toInt
    val fraudIdx = rnd.shuffle((0 until n).toVector).take(fraudCount).toSet
    val cards = math.max(1, n / 20)

    Vector.tabulate(n) { i =>
      val fraud = fraudIdx.contains(i)
      val day = rnd.nextInt(Days)
      // La fraude est plus fréquente la nuit et à l'étranger, sans l'être exclusivement :
      // des achats légitimes ont aussi lieu de nuit (5 %) ou en voyage (4 %).
      val night = if (fraud) rnd.nextDouble() < 0.7 else rnd.nextDouble() < 0.05
      val hour = if (night) rnd.nextInt(6) else 6 + rnd.nextInt(16)
      val second = rnd.nextInt(3600)
      val foreign = if (fraud) rnd.nextDouble() < 0.7 else rnd.nextDouble() < 0.04
      val country = if (foreign) pick(rnd, ForeignCountries) else HomeCountry
      val base = math.exp(3.0 + 0.9 * rnd.nextGaussian()) // log-normale, médiane ~20
      val amount = round2(if (fraud) base * (3.0 + 5.0 * rnd.nextDouble()) else base)

      Transaction(
        transactionId = s"tx-$seed-$i",
        cardId = f"card-${rnd.nextInt(cards)}%06d",
        merchantId = f"m-${rnd.nextInt(500)}%04d",
        category = pick(rnd, Categories),
        country = country,
        ts = StartEpoch + day * SecondsPerDay + hour * 3600L + second,
        amount = amount,
        isFraud = fraud
      )
    }
  }

  private def pick[A](rnd: Random, xs: Vector[A]): A = xs(rnd.nextInt(xs.size))

  private def round2(x: Double): Double = math.round(x * 100.0) / 100.0
}
