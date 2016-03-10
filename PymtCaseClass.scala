/*
 * Represents each Gdelt record as a Scala case class.
 * One physical row in C* per Gdelt record, with separate columns for each column.
 *
 * Note: This is not a workable model for a large number of tables/datasets, or
 * for a large number of columns.
 * - Cassandra uses memory for each table, and most people use very few tables;
 * - Defining all the columns is really tedious, as you can see here
 */

import com.datastax.driver.core.Row
import com.opencsv.CSVReader
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.io.{BufferedReader, FileReader}
import org.joda.time.DateTime
import play.api.libs.iteratee.Iteratee
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


case class PymtModel(
                      coveredRecipientType:Option[String],
                      teachingHospitalId:Option[String],
                      teachingHospitalName:Option[String],
                      physicianProfileId:Option[String],
                      physicianFirstName:Option[String],
                      physicianMiddleName:Option[String],
                      physicianLastName:Option[String],
                      physicianNameSuffix:Option[String],
                      recipientPrimaryBusinessStreetAddressLine1:Option[String],
                      recipientPrimaryBusinessStreetAddressLine2:Option[String],
                      recipientCity:Option[String],
                      recipientState:Option[String],
                      recipientZipCode:Option[String],
                      recipientCountry:Option[String],
                      recipientProvince:Option[String],
                      recipientPostalCode:Option[String],
                      physicianPrimaryType:Option[String],
                      physicianSpecialty:Option[String],
                      extra0: PymtModelExtra0,
                      extra1: PymtModelExtra1,
                      recordId:String,
                      extra2: PymtModelExtra2
                    )
case class PymtModelExtra0(
                            physicianLicenseStateCode1:Option[String],
                            physicianLicenseStateCode2:Option[String],
                            physicianLicenseStateCode3:Option[String],
                            physicianLicenseStateCode4:Option[String],
                            physicianLicenseStateCode5:Option[String]
                          )
case class PymtModelExtra1(
                            submittingApplicableManufacturerOrApplicableGpoName:Option[String],
                            applicableManufacturerOrApplicableGpoMakingPaymentId:Option[String],
                            applicableManufacturerOrApplicableGpoMakingPaymentName:Option[String],
                            applicableManufacturerOrApplicableGpoMakingPaymentState:Option[String],
                            applicableManufacturerOrApplicableGpoMakingPaymentCountry:Option[String],
                            totalAmountOfPaymentUsdollars:Double,
                            dateOfPayment:Option[String],
                            numberOfPaymentsIncludedInTotalAmount:Double,
                            formOfPaymentOrTransferOfValue:Option[String],
                            natureOfPaymentOrTransferOfValue:Option[String],
                            cityOfTravel:Option[String],
                            stateOfTravel:Option[String],
                            countryOfTravel:Option[String],
                            thirdPartyEqualsCoveredRecipientIndicator:Option[String],
                            physicianOwnershipIndicator:Option[String],
                            nameOfThirdPartyEntityReceivingPaymentOrTransferOfValue:Option[String],
                            charityIndicator:Option[String],
                            thirdPartyPaymentRecipientIndicator:Option[String],
                            contextualInformation:Option[String],
                            delayInPublicationIndicator:Option[String]
                          )
case class PymtModelExtra2(
                            disputeStatusForPublication:Option[String],
                            productIndicator:Option[String],
                            nameOfAssociatedCoveredDrugOrBiological1:Option[String],
                            nameOfAssociatedCoveredDrugOrBiological2:Option[String],
                            nameOfAssociatedCoveredDrugOrBiological3:Option[String],
                            nameOfAssociatedCoveredDrugOrBiological4:Option[String],
                            nameOfAssociatedCoveredDrugOrBiological5:Option[String],
                            ndcOfAssociatedCoveredDrugOrBiological1:Option[String],
                            ndcOfAssociatedCoveredDrugOrBiological2:Option[String],
                            ndcOfAssociatedCoveredDrugOrBiological3:Option[String],
                            ndcOfAssociatedCoveredDrugOrBiological4:Option[String],
                            ndcOfAssociatedCoveredDrugOrBiological5:Option[String],
                            nameOfAssociatedCoveredDeviceOrMedicalSupply1:Option[String],
                            nameOfAssociatedCoveredDeviceOrMedicalSupply2:Option[String],
                            nameOfAssociatedCoveredDeviceOrMedicalSupply3:Option[String],
                            nameOfAssociatedCoveredDeviceOrMedicalSupply4:Option[String],
                            nameOfAssociatedCoveredDeviceOrMedicalSupply5:Option[String],
                            programYear:Option[String],
                            paymentPublicationDate:Option[String]
                          )


trait PymtRecordBase[G <: CassandraTable[G, PymtModel]] extends CassandraTable[G, PymtModel] {
  object coveredRecipientType extends OptionalStringColumn(this)
  object teachingHospitalId extends OptionalStringColumn(this)
  object teachingHospitalName extends OptionalStringColumn(this)
  object physicianProfileId extends OptionalStringColumn(this)
  object physicianFirstName extends OptionalStringColumn(this)
  object physicianMiddleName extends OptionalStringColumn(this)
  object physicianLastName extends OptionalStringColumn(this)
  object physicianNameSuffix extends OptionalStringColumn(this)
  object recipientPrimaryBusinessStreetAddressLine1 extends OptionalStringColumn(this)
  object recipientPrimaryBusinessStreetAddressLine2 extends OptionalStringColumn(this)
  object recipientCity extends OptionalStringColumn(this)
  //object recipientState extends OptionalStringColumn(this)
  object recipientZipCode extends OptionalStringColumn(this)
  object recipientCountry extends OptionalStringColumn(this)
  object recipientProvince extends OptionalStringColumn(this)
  object recipientPostalCode extends OptionalStringColumn(this)
  object physicianPrimaryType extends OptionalStringColumn(this)
  object physicianSpecialty extends OptionalStringColumn(this)
  object physicianLicenseStateCode1 extends OptionalStringColumn(this)
  object physicianLicenseStateCode2 extends OptionalStringColumn(this)
  object physicianLicenseStateCode3 extends OptionalStringColumn(this)
  object physicianLicenseStateCode4 extends OptionalStringColumn(this)
  object physicianLicenseStateCode5 extends OptionalStringColumn(this)
  object submittingApplicableManufacturerOrApplicableGpoName extends OptionalStringColumn(this)
  object applicableManufacturerOrApplicableGpoMakingPaymentId extends OptionalStringColumn(this)
  object applicableManufacturerOrApplicableGpoMakingPaymentName extends OptionalStringColumn(this)
  object applicableManufacturerOrApplicableGpoMakingPaymentState extends OptionalStringColumn(this)
  object applicableManufacturerOrApplicableGpoMakingPaymentCountry extends OptionalStringColumn(this)
  object totalAmountOfPaymentUsdollars extends DoubleColumn(this)
  object dateOfPayment extends OptionalStringColumn(this)
  object numberOfPaymentsIncludedInTotalAmount extends DoubleColumn(this)
  object formOfPaymentOrTransferOfValue extends OptionalStringColumn(this)
  object natureOfPaymentOrTransferOfValue extends OptionalStringColumn(this)
  object cityOfTravel extends OptionalStringColumn(this)
  object stateOfTravel extends OptionalStringColumn(this)
  object countryOfTravel extends OptionalStringColumn(this)
  object thirdPartyEqualsCoveredRecipientIndicator extends OptionalStringColumn(this)
  object physicianOwnershipIndicator extends OptionalStringColumn(this)
  object nameOfThirdPartyEntityReceivingPaymentOrTransferOfValue extends OptionalStringColumn(this)
  object charityIndicator extends OptionalStringColumn(this)
  object thirdPartyPaymentRecipientIndicator extends OptionalStringColumn(this)
  object contextualInformation extends OptionalStringColumn(this)
  object delayInPublicationIndicator extends OptionalStringColumn(this)
  //object recordId extends OptionalStringColumn(this)
  object disputeStatusForPublication extends OptionalStringColumn(this)
  object productIndicator extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDrugOrBiological1 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDrugOrBiological2 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDrugOrBiological3 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDrugOrBiological4 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDrugOrBiological5 extends OptionalStringColumn(this)
  object ndcOfAssociatedCoveredDrugOrBiological1 extends OptionalStringColumn(this)
  object ndcOfAssociatedCoveredDrugOrBiological2 extends OptionalStringColumn(this)
  object ndcOfAssociatedCoveredDrugOrBiological3 extends OptionalStringColumn(this)
  object ndcOfAssociatedCoveredDrugOrBiological4 extends OptionalStringColumn(this)
  object ndcOfAssociatedCoveredDrugOrBiological5 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDeviceOrMedicalSupply1 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDeviceOrMedicalSupply2 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDeviceOrMedicalSupply3 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDeviceOrMedicalSupply4 extends OptionalStringColumn(this)
  object nameOfAssociatedCoveredDeviceOrMedicalSupply5 extends OptionalStringColumn(this)
  object programYear extends OptionalStringColumn(this)
  object paymentPublicationDate extends OptionalStringColumn(this)

  override def fromRow(row: Row): PymtModel =
    PymtModel(
      coveredRecipientType(row),
      teachingHospitalId(row),
      teachingHospitalName(row),
      physicianProfileId(row),
      physicianFirstName(row),
      physicianMiddleName(row),
      physicianLastName(row),
      physicianNameSuffix(row),
      recipientPrimaryBusinessStreetAddressLine1(row),
      recipientPrimaryBusinessStreetAddressLine2(row),
      recipientCity(row),
      //recipientState(row),
      None,
      recipientZipCode(row),
      recipientCountry(row),
      recipientProvince(row),
      recipientPostalCode(row),
      physicianPrimaryType(row),
      physicianSpecialty(row),
      PymtModelExtra0(
        physicianLicenseStateCode1(row),
        physicianLicenseStateCode2(row),
        physicianLicenseStateCode3(row),
        physicianLicenseStateCode4(row),
        physicianLicenseStateCode5(row)
      ),
      PymtModelExtra1(
        submittingApplicableManufacturerOrApplicableGpoName(row),
        applicableManufacturerOrApplicableGpoMakingPaymentId(row),
        applicableManufacturerOrApplicableGpoMakingPaymentName(row),
        applicableManufacturerOrApplicableGpoMakingPaymentState(row),
        applicableManufacturerOrApplicableGpoMakingPaymentCountry(row),
        totalAmountOfPaymentUsdollars(row),
        dateOfPayment(row),
        numberOfPaymentsIncludedInTotalAmount(row),
        formOfPaymentOrTransferOfValue(row),
        natureOfPaymentOrTransferOfValue(row),
        cityOfTravel(row),
        stateOfTravel(row),
        countryOfTravel(row),
        thirdPartyEqualsCoveredRecipientIndicator(row),
        physicianOwnershipIndicator(row),
        nameOfThirdPartyEntityReceivingPaymentOrTransferOfValue(row),
        charityIndicator(row),
        thirdPartyPaymentRecipientIndicator(row),
        contextualInformation(row),
        delayInPublicationIndicator(row)
      ),
      //recordId(row),
      "",
      PymtModelExtra2(
      disputeStatusForPublication(row),
      productIndicator(row),
      nameOfAssociatedCoveredDrugOrBiological1(row),
      nameOfAssociatedCoveredDrugOrBiological2(row),
      nameOfAssociatedCoveredDrugOrBiological3(row),
      nameOfAssociatedCoveredDrugOrBiological4(row),
      nameOfAssociatedCoveredDrugOrBiological5(row),
      ndcOfAssociatedCoveredDrugOrBiological1(row),
      ndcOfAssociatedCoveredDrugOrBiological2(row),
      ndcOfAssociatedCoveredDrugOrBiological3(row),
      ndcOfAssociatedCoveredDrugOrBiological4(row),
      ndcOfAssociatedCoveredDrugOrBiological5(row),
      nameOfAssociatedCoveredDeviceOrMedicalSupply1(row),
      nameOfAssociatedCoveredDeviceOrMedicalSupply2(row),
      nameOfAssociatedCoveredDeviceOrMedicalSupply3(row),
      nameOfAssociatedCoveredDeviceOrMedicalSupply4(row),
      nameOfAssociatedCoveredDeviceOrMedicalSupply5(row),
      programYear(row),
      paymentPublicationDate(row)
    )
  )
}

sealed class PymtRecord extends PymtRecordBase[PymtRecord] {
  object recordId extends StringColumn(this) with PartitionKey[String]
  object recipientState extends OptionalStringColumn(this)

  override def fromRow(row: Row): PymtModel =
    super.fromRow(row).copy(
                            recordId = recordId(row),
                            recipientState= recipientState(row))


}

// NOTE: default CQL port is 9042
trait LocalConnector extends SimpleCassandraConnector {
  val keySpace = "test"
}

object PymtRecord extends PymtRecord with LocalConnector {
  override val tableName = "pymt"

  def insertRecords(records: Seq[PymtModel]): Future[ResultSet] = {
    // NOTE: Apparently this is an anti-pattern, because a BATCH statement
    // forces a single coordinator to handle everything, whereas if they were
    // individual writes, they could go to the right node, skipping a hop.
    // However this test is done on localhost, so it probably doesn't matter as much.
    val batch = UnloggedBatchStatement()
    records.foreach { record =>
      batch.add(insert.value(_.coveredRecipientType, record.coveredRecipientType)
        .value(_.teachingHospitalId, record.teachingHospitalId)
        .value(_.teachingHospitalName, record.teachingHospitalName)
        .value(_.physicianProfileId, record.physicianProfileId)
        .value(_.physicianFirstName, record.physicianFirstName)
        .value(_.physicianMiddleName, record.physicianMiddleName)
        .value(_.physicianLastName, record.physicianLastName)
        .value(_.physicianNameSuffix, record.physicianNameSuffix)
        .value(_.recipientPrimaryBusinessStreetAddressLine1, record.recipientPrimaryBusinessStreetAddressLine1)
        .value(_.recipientPrimaryBusinessStreetAddressLine2, record.recipientPrimaryBusinessStreetAddressLine2)
        .value(_.recipientCity, record.recipientCity)
        .value(_.recipientState, record.recipientState)
        .value(_.recipientZipCode, record.recipientZipCode)
        .value(_.recipientCountry, record.recipientCountry)
        .value(_.recipientProvince, record.recipientProvince)
        .value(_.recipientPostalCode, record.recipientPostalCode)
        .value(_.physicianPrimaryType, record.physicianPrimaryType)
        .value(_.physicianSpecialty, record.physicianSpecialty)
        .value(_.physicianLicenseStateCode1, record.extra0.physicianLicenseStateCode1)
        .value(_.physicianLicenseStateCode2, record.extra0.physicianLicenseStateCode2)

        .value(_.physicianLicenseStateCode3, record.extra0.physicianLicenseStateCode3)
        .value(_.physicianLicenseStateCode4, record.extra0.physicianLicenseStateCode4)
        .value(_.physicianLicenseStateCode5, record.extra0.physicianLicenseStateCode5)
        .value(_.submittingApplicableManufacturerOrApplicableGpoName, record.extra1.submittingApplicableManufacturerOrApplicableGpoName)
        .value(_.applicableManufacturerOrApplicableGpoMakingPaymentId, record.extra1.applicableManufacturerOrApplicableGpoMakingPaymentId)
        .value(_.applicableManufacturerOrApplicableGpoMakingPaymentName, record.extra1.applicableManufacturerOrApplicableGpoMakingPaymentName)
        .value(_.applicableManufacturerOrApplicableGpoMakingPaymentState, record.extra1.applicableManufacturerOrApplicableGpoMakingPaymentState)
        .value(_.applicableManufacturerOrApplicableGpoMakingPaymentCountry, record.extra1.applicableManufacturerOrApplicableGpoMakingPaymentCountry)
        .value(_.totalAmountOfPaymentUsdollars, record.extra1.totalAmountOfPaymentUsdollars)
        .value(_.dateOfPayment, record.extra1.dateOfPayment)
        .value(_.numberOfPaymentsIncludedInTotalAmount, record.extra1.numberOfPaymentsIncludedInTotalAmount)
        .value(_.formOfPaymentOrTransferOfValue, record.extra1.formOfPaymentOrTransferOfValue)
        .value(_.natureOfPaymentOrTransferOfValue, record.extra1.natureOfPaymentOrTransferOfValue)
        .value(_.cityOfTravel, record.extra1.cityOfTravel)
        .value(_.stateOfTravel, record.extra1.stateOfTravel)
        .value(_.countryOfTravel, record.extra1.countryOfTravel)
        .value(_.thirdPartyEqualsCoveredRecipientIndicator, record.extra1.thirdPartyEqualsCoveredRecipientIndicator)
        .value(_.physicianOwnershipIndicator, record.extra1.physicianOwnershipIndicator)
        .value(_.nameOfThirdPartyEntityReceivingPaymentOrTransferOfValue, record.extra1.nameOfThirdPartyEntityReceivingPaymentOrTransferOfValue)

        .value(_.charityIndicator, record.extra1.charityIndicator)
        .value(_.thirdPartyPaymentRecipientIndicator, record.extra1.thirdPartyPaymentRecipientIndicator)
        .value(_.contextualInformation, record.extra1.contextualInformation)
        .value(_.delayInPublicationIndicator, record.extra1.delayInPublicationIndicator)
        //CHECK
        .value(_.recordId, record.recordId)
        .value(_.disputeStatusForPublication, record.extra2.disputeStatusForPublication)
        .value(_.productIndicator, record.extra2.productIndicator)
        .value(_.nameOfAssociatedCoveredDrugOrBiological1, record.extra2.nameOfAssociatedCoveredDrugOrBiological1)
        .value(_.nameOfAssociatedCoveredDrugOrBiological2, record.extra2.nameOfAssociatedCoveredDrugOrBiological2)
        .value(_.nameOfAssociatedCoveredDrugOrBiological3, record.extra2.nameOfAssociatedCoveredDrugOrBiological3)
        .value(_.nameOfAssociatedCoveredDrugOrBiological4, record.extra2.nameOfAssociatedCoveredDrugOrBiological4)
        .value(_.nameOfAssociatedCoveredDrugOrBiological5, record.extra2.nameOfAssociatedCoveredDrugOrBiological5)
        .value(_.ndcOfAssociatedCoveredDrugOrBiological1, record.extra2.ndcOfAssociatedCoveredDrugOrBiological1)
        .value(_.ndcOfAssociatedCoveredDrugOrBiological2, record.extra2.ndcOfAssociatedCoveredDrugOrBiological2)
        .value(_.ndcOfAssociatedCoveredDrugOrBiological3, record.extra2.ndcOfAssociatedCoveredDrugOrBiological3)
        .value(_.ndcOfAssociatedCoveredDrugOrBiological4, record.extra2.ndcOfAssociatedCoveredDrugOrBiological4)
        .value(_.ndcOfAssociatedCoveredDrugOrBiological5, record.extra2.ndcOfAssociatedCoveredDrugOrBiological5)
        .value(_.nameOfAssociatedCoveredDeviceOrMedicalSupply1, record.extra2.nameOfAssociatedCoveredDeviceOrMedicalSupply1)
        .value(_.nameOfAssociatedCoveredDeviceOrMedicalSupply2, record.extra2.nameOfAssociatedCoveredDeviceOrMedicalSupply2)
        .value(_.nameOfAssociatedCoveredDeviceOrMedicalSupply3, record.extra2.nameOfAssociatedCoveredDeviceOrMedicalSupply3)
        .value(_.nameOfAssociatedCoveredDeviceOrMedicalSupply4, record.extra2.nameOfAssociatedCoveredDeviceOrMedicalSupply4)
        .value(_.nameOfAssociatedCoveredDeviceOrMedicalSupply5, record.extra2.nameOfAssociatedCoveredDeviceOrMedicalSupply5)
        .value(_.programYear, record.extra2.programYear)
        .value(_.paymentPublicationDate, record.extra2.paymentPublicationDate)
      )
    }
    batch.future()
  }

  def elapsed[A](f: => A): (A, Double) = {
    val startTime = System.currentTimeMillis
    val ret = f
    val elapsed = (System.currentTimeMillis - startTime) / 1000.0
    (ret, elapsed)
  }
}

/**
 * Run this to set up the test keyspace and table on localhost/9042.
 */
object PymtCaseClassSetup extends App with LocalConnector {
  println("Setting up keyspace and table...")
  println(Await.result(PymtRecord.create.future(), 5000 millis))
  println("...done")
}

/**
 * Run this to import the rows into local Casandra.
 */
object PymtCaseClassImporter extends App with LocalConnector {
  import CsvParsingUtils._

  val filePath = Try(args(0)).getOrElse {
    println("Usage: pass the gdelt CSV file as the first arg")
    sys.exit(0)
  }

  val reader = new CSVReader(new BufferedReader(new FileReader(filePath)), ',')
  val fileIter = new PymtReader(reader)

  // Parse each line into a case class
  println("Ingesting, each dot equals 1000 records...")
  var recordCount = 0L

  var groupCount = 0
  val (_,elapsed) = PymtRecord.elapsed{
    while (fileIter.hasNext){
      if (groupCount <1000){
        groupCount+=1
        recordCount +=1
        val record = l2.next()
        Await.result(PymtRecord.insertRecords(record), 10 seconds)
        print(".")

      }else{
        writeGroup()
        groupCount+=1
        recordCount +=1
        val record = l2.next()
        Await.result(PymtRecord.insertRecords(record), 10 seconds)
      }
    }
  }
  val (_, elapsed) = PymtRecord.elapsed {
    fileIter.grouped(1000)
            .foreach { records =>
              recordCount += records.length
              Await.result(PymtRecord.insertRecords(records), 10 seconds)
              print(".")
            }
  }
  println(s"Done in ${elapsed} secs, ${recordCount / elapsed} records/sec")
}

/**
 * Run this to time queries against the imported records
 */
object PymtCaseClassQuery extends App with LocalConnector {
  println("Querying every column (full export)...")
  val (result, elapsed) = PymtRecord.elapsed {
    val f = PymtRecord.select.
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result in $elapsed seconds")

  println("Querying just monthYear column out of 60...")
  val (result2, elapsed2) = PymtRecord.elapsed {
    val f = PymtRecord.select(_.coveredRecipientType).
              fetchEnumerator run (Iteratee.fold(0) { (acc, elt: Any) => acc + 1 })
    Await.result(f, 5000 seconds)
  }
  println(s".... got count of $result2 in $elapsed2 seconds")
}