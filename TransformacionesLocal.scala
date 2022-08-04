package Recogida.Common

import Recogida.spark
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.storage.StorageLevel
import spark.implicits._

import java.sql.Timestamp
import java.util.Date

object TransformacionesLocal extends {

  def RecogidaInicial(date: String): Unit = {
    val DWE_VM_UAACTIVI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UAACTIVI_*")
    val DWE_VM_UGACTMUN = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UGACTMUN_*")
    val DWE_VM_VOLUMENS = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_VOLUMENS_*")
    val DWE_VM_UNIDADMI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UNIDADMI_*")
    val DWE_VM_UGACTIVI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UGACTIVI_*")
    val DWE_VM_UFUGACTI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UFUGACTI_*")
    val DWE_VM_UFTRGMUN = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UFTRGMUN_*")
    val DWE_VM_TPRECOGI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TPRECOGI_*")
    val DWE_VM_TIPOLFAC = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TIPOLFAC_*")
    val DWE_VM_TIPOLENT = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TIPOLENT_*")
    val DWE_VM_POBPERST = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_POBPERST_*")
    val DWE_VM_MEDPERST = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_MEDPERST_*")
    val DWE_VM_ENTLTPRE = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ENTLTPRE_*")
    val DWE_VM_ENTLOCAL = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ENTLOCAL_*")
    val DWE_VM_ELTREPOB = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ELTREPOB_*")
    val DWE_VM_ELTREMED = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ELTREMED_*")
    val DWE_VM_COMUAUTO = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_COMUAUTO_*")
    val DWE_SGR_MU_ASIG_OPERADORES_UF = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGR_MU_ASIG_OPERADORES_UF_*")
    val DWE_SGR_MU_ASIG_OPERADORES_UTE = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGR_MU_ASIG_OPERADORES_UTE_*")
    val DWE_SGE_SAP_PROVEEDORES = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGE_SAP_PROVEEDORES_*")

    //DWE_VM_UAACTIVI.show()
    //DWE_VM_UGACTMUN.show()

    //Filtramos (lt = less than, gt = greater than, || = or)

    //Primero probamos de una en una
    //val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30")))).show()
    //val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-xx-xx)) && DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("HASTA_DT").gt("2017-xx-xx))
    //val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("HASTA_DT").isNull).show()

    //Toda junta
      val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30"))) || (DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_TIPOLFAC("HASTA_DT").gt("2017-08-01")) || (DWE_VM_TIPOLFAC("HASTA_DT").isNull))).show()
      val DWE_VM_UAACTIVI2 = DWE_VM_UAACTIVI.filter((DWE_VM_UAACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UAACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UAACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UAACTIVI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UAACTIVI("HASTA_DT").isNull)))
  }



  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
