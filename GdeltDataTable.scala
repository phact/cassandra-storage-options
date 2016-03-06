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


object GdeltSchema {
  val schema = Seq(
    VectorInfo("globalEventId", classOf[String]),
    VectorInfo("sqlDate",       classOf[String]),
    VectorInfo("monthYear",     classOf[Int]),
    VectorInfo("year",          classOf[Int]),
    VectorInfo("fractionDate",  classOf[Double]),
    VectorInfo("a1Code",    classOf[String]),
    VectorInfo("a1Name",    classOf[String]),
    VectorInfo("a1CountryCode",    classOf[String]),
    VectorInfo("a1KnownGroupCode", classOf[String]),
    VectorInfo("a1EthnicCode",    classOf[String]),
    VectorInfo("a1Religion1Code",    classOf[String]),
    VectorInfo("a1Religion2Code",    classOf[String]),
    VectorInfo("a1Type1Code",    classOf[String]),
    VectorInfo("a1Type2Code",    classOf[String]),
    VectorInfo("a1Type3Code",    classOf[String]),
    VectorInfo("a2Code",    classOf[String]),
    VectorInfo("a2Name",    classOf[String]),
    VectorInfo("a2CountryCode",    classOf[String]),
    VectorInfo("a2KnownGroupCode", classOf[String]),
    VectorInfo("a2EthnicCode",    classOf[String]),
    VectorInfo("a2Religion1Code",  classOf[String]),
    VectorInfo("a2Religion2Code",  classOf[String]),
    VectorInfo("a2Type1Code",  classOf[String]),
    VectorInfo("a2Type2Code",  classOf[String]),
    VectorInfo("a2Type3Code",  classOf[String]),

    // Cols 25-34
    VectorInfo("isRootEvent",  classOf[Int]),
    VectorInfo("eventCode",  classOf[String]),
    VectorInfo("eventBaseCode",  classOf[String]),
    VectorInfo("eventRootCode",  classOf[String]),
    VectorInfo("quadClass",  classOf[Int]),
    VectorInfo("goldsteinScale",  classOf[Double]),
    VectorInfo("numMentions",  classOf[Int]),
    VectorInfo("numSources",  classOf[Int]),
    VectorInfo("numArticles",  classOf[Int]),
    VectorInfo("avgTone",  classOf[Double]),

    // Cols 35-41
    VectorInfo("a1geoType",  classOf[String]),
    VectorInfo("a1fullName",  classOf[String]),
    VectorInfo("a1gcountryCode",  classOf[String]),
    VectorInfo("a1adm1Code",  classOf[String]),
    VectorInfo("a1lat",  classOf[Double]),
    VectorInfo("a1long",  classOf[Double]),
    VectorInfo("a1featureID",  classOf[String]),

    // Cols 42-48
    VectorInfo("a2geoType",  classOf[String]),
    VectorInfo("a2fullName",  classOf[String]),
    VectorInfo("a2gcountryCode",  classOf[String]),
    VectorInfo("a2adm1Code",  classOf[String]),
    VectorInfo("a2lat",  classOf[Double]),
    VectorInfo("a2long",  classOf[Double]),
    VectorInfo("a2featureID",  classOf[String]),

    // Cols 49-55
    VectorInfo("actgeoType",  classOf[String]),
    VectorInfo("actfullName",  classOf[String]),
    VectorInfo("actgcountryCode",  classOf[String]),
    VectorInfo("actadm1Code",  classOf[String]),
    VectorInfo("actlat",  classOf[Double]),
    VectorInfo("actlong",  classOf[Double]),
    VectorInfo("actfeatureID",  classOf[String]),

    // Cols 56-59
    VectorInfo("dateAdded",  classOf[String]),
    VectorInfo("a1fullLocation",  classOf[String]),
    VectorInfo("a2fullLocation",  classOf[String]),
    VectorInfo("actfullLocation",  classOf[String])
  )
}

/**
 * Run this to import the rows into local Casandra.
 */
object GdeltDataTableImporter extends App with LocalConnector {
  import CsvParsingUtils._

  val gdeltFilePath = Try(args(0)).getOrElse {
    println("Usage: pass the gdelt CSV file as the first arg")
    sys.exit(0)
  }

  val reader = new CSVReader(new BufferedReader(new FileReader(gdeltFilePath)), '\t')
  val lineIter = new LineReader(reader)

  val r2 = new CSVReader(new BufferedReader(new FileReader(gdeltFilePath)), '\t')
  val l2 = new LineReader(r2)





  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  val builder = new RowToVectorBuilder(GdeltSchema.schema)
  var recordCount = 0L
  var rowId = 0
  var shard = 0

  var groupCount = 0


  def writeGroup ()= {
    val columnBytes = builder.convertToBytes()
    groupCount=0
    Await.result(DataTableRecord.insertOneRow("gdelt", 0, shard, rowId, columnBytes), 10 seconds)
    print(".")
    builder.reset()
    rowId += 1
    if (rowId >= 100) {
      // After 100 * 1000 rows, go to the next shard.
      shard += 1
      rowId = 0
    }
  }

  val (_, elapsed) = GdeltRecord.elapsed {
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
      }
      /*
      line.toSeq.map(ArrayStringRowReader).foreach{r =>
        lineCount = lineCount + 1
      }
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
    GdeltSchema.schema.map(_.name).zip(builder.builders).foreach { case (name, builder) =>
      //println(s"  name: $name \t#NAbits: ${builder.naMask.size}")
      println(s"  name: $name ")
    }
  }
}

object GdeltDataTableQuery extends App with LocalConnector {
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
  val (result, elapsed) = GdeltRecord.elapsed {
    (0 to 40).foldLeft(0) { (timerAcc, shard) =>
      val f = DataTableRecord.readAllColumns("gdelt", 0, shard) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  /*
                  println("Column/Row combination"+x)
                  if (x._1=="year"){
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
  val (result2, elapsed2) = GdeltRecord.elapsed {
    (0 to 40).foldLeft(0) { (acc, shard) =>
      val f = DataTableRecord.readSelectColumns("gdelt", 0, shard, List("monthYear")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = FiloVector[Int](x._3)
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
  val (result3, elapsed3) = GdeltRecord.elapsed {
    val myCount = HashMap.empty[Int, Int].withDefaultValue(0)
    (0 to 40).foreach { shard =>
      val f = DataTableRecord.readSelectColumns("gdelt", 0, shard, List("monthYear")) run (
                Iteratee.fold(0) { (acc, x: DataTableRecord.ColRowBytes) =>
                  val col = FiloVector[Int](x._3)
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
