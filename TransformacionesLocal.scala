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

    DWE_VM_UAACTIVI.show(5)
    DWE_VM_UGACTMUN.show(5)
  }



  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilos


}
