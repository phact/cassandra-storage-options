import com.opencsv.CSVReader
import java.io.{BufferedReader, FileReader}
import java.nio.ByteBuffer
import org.joda.time.DateTime
import org.velvia.filo._
import org.velvia.filo.VectorInfo
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


object PymtSchema {
  val schema = Seq(
    VectorInfo("covered_recipient_type", classOf[String]),
    VectorInfo("teaching_hospital_id", classOf[String]),
    VectorInfo("teaching_hospital_name", classOf[String]),
    VectorInfo("physician_profile_id", classOf[String]),
    VectorInfo("physician_first_name", classOf[String]),
    VectorInfo("physician_middle_name", classOf[String]),
    VectorInfo("physician_last_name", classOf[String]),
    VectorInfo("physician_name_suffix", classOf[String]),
    VectorInfo("recipient_primary_business_street_address_line1", classOf[String]),
    VectorInfo("recipient_primary_business_street_address_line2", classOf[String]),
    VectorInfo("recipient_city", classOf[String]),
    VectorInfo("recipient_state", classOf[String]),
    VectorInfo("recipient_zip_code", classOf[String]),
    VectorInfo("recipient_country", classOf[String]),
    VectorInfo("recipient_province", classOf[String]),
    VectorInfo("recipient_postal_code", classOf[String]),
    VectorInfo("physician_primary_type", classOf[String]),
    VectorInfo("physician_specialty", classOf[String]),
    VectorInfo("physician_license_state_code1", classOf[String]),
    VectorInfo("physician_license_state_code2", classOf[String]),
    VectorInfo("physician_license_state_code3", classOf[String]),
    VectorInfo("physician_license_state_code4", classOf[String]),
    VectorInfo("physician_license_state_code5", classOf[String]),
    VectorInfo("submitting_applicable_manufacturer_or_applicable_gpo_name", classOf[String]),
    VectorInfo("applicable_manufacturer_or_applicable_gpo_making_payment_id", classOf[String]),
    VectorInfo("applicable_manufacturer_or_applicable_gpo_making_payment_name", classOf[String]),
    VectorInfo("applicable_manufacturer_or_applicable_gpo_making_payment_state", classOf[String]),
    VectorInfo("applicable_manufacturer_or_applicable_gpo_making_payment_country", classOf[String]),
    VectorInfo("total_amount_of_payment_usdollars", classOf[Double]),
    VectorInfo("date_of_payment", classOf[String]),
    VectorInfo("number_of_payments_included_in_total_amount", classOf[Double]),
    VectorInfo("form_of_payment_or_transfer_of_value", classOf[String]),
    VectorInfo("nature_of_payment_or_transfer_of_value", classOf[String]),
    VectorInfo("city_of_travel", classOf[String]),
    VectorInfo("state_of_travel", classOf[String]),
    VectorInfo("country_of_travel", classOf[String]),
    VectorInfo("third_party_equals_covered_recipient_indicator", classOf[String]),
    VectorInfo("physician_ownership_indicator", classOf[String]),
    VectorInfo("name_of_third_party_entity_receiving_payment_or_transfer_of_value", classOf[String]),
    VectorInfo("charity_indicator", classOf[String]),
    VectorInfo("third_party_payment_recipient_indicator", classOf[String]),
    VectorInfo("contextual_information", classOf[String]),
    VectorInfo("delay_in_publication_indicator", classOf[String]),
    VectorInfo("record_id", classOf[String]),
    VectorInfo("dispute_status_for_publication", classOf[String]),
    VectorInfo("product_indicator", classOf[String]),
    VectorInfo("name_of_associated_covered_drug_or_biological1", classOf[String]),
    VectorInfo("name_of_associated_covered_drug_or_biological2", classOf[String]),
    VectorInfo("name_of_associated_covered_drug_or_biological3", classOf[String]),
    VectorInfo("name_of_associated_covered_drug_or_biological4", classOf[String]),
    VectorInfo("name_of_associated_covered_drug_or_biological5", classOf[String]),
    VectorInfo("ndc_of_associated_covered_drug_or_biological1", classOf[String]),
    VectorInfo("ndc_of_associated_covered_drug_or_biological2", classOf[String]),
    VectorInfo("ndc_of_associated_covered_drug_or_biological3", classOf[String]),
    VectorInfo("ndc_of_associated_covered_drug_or_biological4", classOf[String]),
    VectorInfo("ndc_of_associated_covered_drug_or_biological5", classOf[String]),
    VectorInfo("name_of_associated_covered_device_or_medical_supply1", classOf[String]),
    VectorInfo("name_of_associated_covered_device_or_medical_supply2", classOf[String]),
    VectorInfo("name_of_associated_covered_device_or_medical_supply3", classOf[String]),
    VectorInfo("name_of_associated_covered_device_or_medical_supply4", classOf[String]),
    VectorInfo("name_of_associated_covered_device_or_medical_supply5", classOf[String]),
    VectorInfo("program_year", classOf[String]),
    VectorInfo("payment_publication_date", classOf[String])
  )
}

/**
 * Run this to import the rows into local Casandra.
 */
object PymtTableImporter extends App with LocalConnector {
  import CsvParsingUtils._

  val filePath = Try(args(0)).getOrElse {
    println("Usage: pass the gdelt CSV file as the first arg")
    sys.exit(0)
  }

  //fix for CSV not TSV
  val reader = new CSVReader(new BufferedReader(new FileReader(filePath)), ',')
  val lineIter = new LineReader(reader)

  val r2 = new CSVReader(new BufferedReader(new FileReader(filePath)), ',')
  val l2 = new LineReader(r2)





  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  val builder = new RowToVectorBuilder(PymtSchema.schema)
  var recordCount = 0L
  var rowId = 0
  var shard = 0

  var groupCount = 0

def writeGroup ()= { val columnBytes = builder.convertToBytes() /*
    val chunk = builder
    println(chunk)
    println(columnBytes)

    val columnBytes = GdeltSchema.schema.map(_.name).zip(Seq(chunk)).map { case (VectorInfo(colName, _), bytes) => (colName, bytes) }.toMap
     */

    groupCount=0
    Await.result(DataTableRecord.insertOneRow("pymt", 0, shard, rowId, columnBytes), 10 seconds)
    print(".")
    builder.reset()
    rowId += 1
    if (rowId >= 100) {
      // After 100 * 1000 rows, go to the next shard.
      shard += 1
      rowId = 0
    }
  }

  val (_, elapsed) = PymtRecord.elapsed {
    while(l2.hasNext){
      if (groupCount <1000){
        groupCount+=1
        recordCount +=1
        val record = l2.next()
        builder.addRow(ArrayStringRowReader(record))

      }else{
        writeGroup()
        groupCount+=1
        recordCount +=1
        val record = l2.next()
        builder.addRow(ArrayStringRowReader(record))
        //println("shard: "+shard)
        //println("row: "+rowId)
      } /* line.toSeq.map(ArrayStringRowReader).foreach{r => lineCount = lineCount + 1 }
      */
    }
  }
  writeGroup()
  //println(recordCount)


/*
  val (_, elapsed) = GdeltRecord.elapsed {
    lineIter.grouped(1000)
            .foreach { records =>
              recordCount += records.length
              records.toSeq.map(ArrayStringRowReader).foreach{
                r =>
                  builder.addRow(r)
              }
              //records.foreach { r => builder.addRow(r) }
              val columnBytes = builder.convertToBytes()
              //columnBytes is a Map of column names and bytebuffers. The bytebuffers contain the values for each row for the given column.
              //println("Column names:  " + columnBytes.keys)
              //println("Column names and byte buffer pointers" + columnBytes)
              //println("Values in the byteBuffer for globalEventId: " + FiloVector[String](columnBytes("globalEventId")))
              println("first globalEventId in this group: "+ FiloVector[String](columnBytes("globalEventId")).headOption)
              println("last globalEventId in this group: "+ FiloVector[String](columnBytes("globalEventId")).last)
              //val columnToBytes = builder
              Await.result(DataTableRecord.insertOneRow("gdelt", 0, shard, rowId, columnBytes), 10 seconds)
              //analyzeData()
              print(".")
              builder.reset()
              rowId += 1
              if (rowId >= 100) {
                // After 100 * 1000 rows, go to the next shard.
                shard += 1
                rowId = 0
              }
              //println("shard: "+shard)
              //println("row: "+rowId)
              if (rowId==31 && shard ==4){
                println(records.toSeq.length)
              }
            }
  }
  */
  println(s"Done in ${elapsed} secs, ${recordCount / elapsed} records/sec")
  println(s"shard = $shard   rowId = $rowId")
  //println(s"# of SimpleColumns: ${SimpleEncoders.count}")
  //println(s"# of DictEncodingColumns: ${DictEncodingEncoders.count}")

  private def analyzeData() {
    println("\n---")
    PymtSchema.schema.map(_.name).zip(builder.builders).foreach { case (name, builder) =>
      //println(s"  name: $name \t#NAbits: ${builder.naMask.size}")
      println(s"  name: $name ")
    }
  }
}

object PymtDataTableQuery extends App with LocalConnector {
  import scala.concurrent.ExecutionContext.Implicits.global
  import collection.mutable.HashMap
  import collection.mutable.{Map => MMap}

  case class RecordCounter(
      maxRowIdMap: MMap[Int, Int] = HashMap.empty.withDefaultValue(0),
      colCount: MMap[String, Int] = HashMap.empty.withDefaultValue(0),
      bytesRead: MMap[String, Long] = HashMap.empty.withDefaultValue(0L)) {
        def addRowIdForShard(shard: Int, rowId: Int) {
          maxRowIdMap(shard) = Math.max(maxRowIdMap(shard), rowId)
        }

        def addColCount(column: String) {
          colCount(column) += 1
        }

        def addColBytes(column: String, bytes: Long) { bytesRead(column) += bytes }
      }

  // NOTE: we are cheating since I know beforehand there are 40 shards.
  // Gather some statistics to make sure we are indeed reading every row and shard
  println("Querying every column (full export)...")
  val counter = RecordCounter()
  val (result, elapsed) = PymtRecord.elapsed {
    (0 to 40).foldLeft(0) { (timerAcc, shard) =>
      val f = DataTableRecord.readAllColumns("pymt", 0, shard) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
/*
                  println("Column/Row combination"+x)
                  if (x._1=="covered_recepient_type"){
                    println("value "+ FiloVector[Int](x._3))
                    println("count: "+ FiloVector[Int](x._3).length)
                  }
*/
                  counter.addRowIdForShard(shard, x._2)
                  counter.addColCount(x._1)
                  counter.addColBytes(x._1, x._3.remaining.toLong)
                  //println(acc)
                  acc + 1
                }
              )
      //println("time so far: "+timerAcc)
      timerAcc + Await.result(f, 5000 seconds)
    }
  }
  println(s".... got count of $result in $elapsed seconds")
  //println(s"column count values: "+ counter.colCount.values)
  println(s"max row Ids "+ counter.maxRowIdMap)
  println(s"sum of row Ids"+ counter.maxRowIdMap.values.sum)
  //println("Shard and column count stats: " + counter)
  println("Total bytes read: " + counter.bytesRead.values.sum)

  //import ColumnParser._

  println("Querying just monthYear column out of 20, counting # of elements...")
  val (result2, elapsed2) = PymtRecord.elapsed {
    (0 to 40).foldLeft(0) { (acc, shard) =>
      val f = DataTableRecord.readSelectColumns("pymt", 0, shard, List("covered_recipient_type")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = FiloVector[String](x._3)
                  var count = 0
                  col.foreach { monthYear => count += 1 }
                  //each should be 1000 except the last since that's how we chunked them on write.
                  //println(count)
                  acc + count
                } )
      acc + Await.result(f, 5000 seconds)
    }
  }
  println(s".... got count of $result2 in $elapsed2 seconds")

  println("Querying just monthYear column out of 20, top K of elements...")
  val (result3, elapsed3) = PymtRecord.elapsed {
    val myCount = HashMap.empty[String, Int].withDefaultValue(0)
    (0 to 40).foreach { shard =>
      val f = DataTableRecord.readSelectColumns("pymt", 0, shard, List("covered_recipient_type")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = FiloVector[String](x._3)
                  col.foreach { monthYear => myCount(monthYear) += 1 }
                  0
                } )
      Await.result(f, 5000 seconds)
    }
    myCount.toSeq.sortBy(_._2).reverse.take(10)
  }
  println(s".... got count of $result3 in $elapsed3 seconds")

  println("All done!")
}
