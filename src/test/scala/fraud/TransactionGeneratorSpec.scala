package fraud

import org.scalatest.funsuite.AnyFunSuite

class TransactionGeneratorSpec extends AnyFunSuite {

  test("même graine, mêmes transactions (critère 3)") {
    val a = TransactionGenerator.generate(n = 1000, seed = 42L, fraudRate = 0.01)
    val b = TransactionGenerator.generate(n = 1000, seed = 42L, fraudRate = 0.01)
    assert(a == b)
  }

  test("graines différentes, transactions différentes") {
    val a = TransactionGenerator.generate(n = 1000, seed = 1L, fraudRate = 0.01)
    val b = TransactionGenerator.generate(n = 1000, seed = 2L, fraudRate = 0.01)
    assert(a != b)
  }

  test("taux de fraude mesuré à ±0,05 point du taux demandé (critère 4)") {
    val txs = TransactionGenerator.generate(n = 200000, seed = 7L, fraudRate = 0.01)
    val measured = txs.count(_.isFraud).toDouble / txs.size
    assert(math.abs(measured - 0.01) <= 0.0005)
  }

  test("identifiants de transaction uniques") {
    val txs = TransactionGenerator.generate(n = 5000, seed = 3L, fraudRate = 0.02)
    assert(txs.map(_.transactionId).distinct.size == txs.size)
  }
}
