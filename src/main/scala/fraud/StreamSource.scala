package fraud

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption}

/** Dépose des micro-batchs JSON (une transaction par ligne) dans le dossier lu par le streaming. */
object StreamSource {

  private def esc(s: String): String = s.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case c    => c.toString
  }

  def toJson(t: Transaction): String =
    s"""{"transaction_id":"${esc(t.transactionId)}","card_id":"${esc(t.cardId)}","merchant_id":"${esc(t.merchantId)}",""" +
      s""""category":"${esc(t.category)}","country":"${esc(t.country)}","ts":"${t.ts}","amount":"${t.amount}","is_fraud":"${t.isFraud}"}"""

  /** Écrit dans un fichier caché puis renomme : Spark ignore les fichiers commençant par un point. */
  def writeBatch(dir: Path, fileName: String, txs: Seq[Transaction]): Unit = {
    val tmp = dir.resolve(s".$fileName.tmp")
    Files.write(tmp, txs.map(toJson).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
    Files.move(tmp, dir.resolve(fileName), StandardCopyOption.ATOMIC_MOVE)
  }
}
