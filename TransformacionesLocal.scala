package Recogida.Common

import Recogida.spark
import org.apache.spark.sql.functions.{col, isnull, lit, to_timestamp}
import org.apache.spark.storage.StorageLevel
import spark.implicits._

import java.sql.Timestamp
import java.util.Date

object TransformacionesLocal extends {

  def RecogidaInicial(date: String): Unit = {
    val DWE_VM_UAACTIVI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UAACTIVI_*")
    val DWE_VM_UGACTMUN = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UGACTMUN_*")
    val DWE_VM_UNIDADMI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UNIDADMI_*")
    val DWE_VM_UGACTIVI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UGACTIVI_*")
    val DWE_VM_UFUGACTI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UFUGACTI_*")
    val DWE_VM_UFTRGMUN = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_UFTRGMUN_*")
    val DWE_VM_TPRECOGI = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TPRECOGI_*")
    val DWE_VM_TIPOLFAC = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TIPOLFAC_*")
    val DWE_VM_TIPOLENT = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_TIPOLENT_*")
    val DWE_VM_POBPERST = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_POBPERST_*")
    val DWE_VM_ENTLTPRE = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ENTLTPRE_*")
    val DWE_VM_ENTLOCAL = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ENTLOCAL_*")
    val DWE_VM_ELTREPOB = spark.read.option("inferSchema", true).option("header", true).csv("DWE_VM_ELTREPOB_*")
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

    //Filters
    val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter((DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30"))))|| (DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_TIPOLFAC("HASTA_DT").gt("2017-07-01")) || (DWE_VM_TIPOLFAC("HASTA_DT").isNull)))
    val DWE_VM_UAACTIVI2 = DWE_VM_UAACTIVI.filter((DWE_VM_UAACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UAACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UAACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UAACTIVI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UAACTIVI("HASTA_DT").isNull)))
    val DWE_VM_UGACTMUN2 = DWE_VM_UGACTMUN.filter((DWE_VM_UGACTMUN("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UGACTMUN("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UGACTMUN("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UGACTMUN("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UGACTMUN("HASTA_DT").isNull)))
    val DWE_VM_UFTRGMUN2 = DWE_VM_UFTRGMUN.filter((DWE_VM_UFTRGMUN("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UFTRGMUN("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UFTRGMUN("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UFTRGMUN("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UFTRGMUN("HASTA_DT").isNull)))
    val DWE_VM_ENTLTPRE2 = DWE_VM_ENTLTPRE.filter((DWE_VM_ENTLTPRE("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_ENTLTPRE("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_ENTLTPRE("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_ENTLTPRE("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_ENTLTPRE("HASTA_DT").isNull)))
    val DWE_VM_ELTREPOB2 = DWE_VM_ELTREPOB.filter((DWE_VM_ELTREPOB("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_ELTREPOB("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_ELTREPOB("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_ELTREPOB("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_ELTREPOB("HASTA_DT").isNull)))
    val DWE_VM_UGACTIVI2 = DWE_VM_UGACTIVI.filter((DWE_VM_UGACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UGACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UGACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UGACTIVI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UGACTIVI("HASTA_DT").isNull)))
    val DWE_VM_UFUGACTI2 = DWE_VM_UFUGACTI.filter((DWE_VM_UFUGACTI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UFUGACTI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UFUGACTI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UFUGACTI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UFUGACTI("HASTA_DT").isNull)))

    //Hacemos los joins (líneas 20 y 27-31)
    DWE_VM_UFUGACTI2.join(DWE_SGE_SAP_PROVEEDORES, DWE_VM_UFUGACTI2("UNFAC_ID") === DWE_SGE_SAP_PROVEEDORES("PROVE_ID"), "right")
    DWE_VM_TIPOLFAC2.join(DWE_VM_ENTLTPRE, DWE_VM_TIPOLFAC2("ELMUN_ID") === DWE_VM_ENTLTPRE("ELMUN_ID"), "right")

    //Triple join (líneas 32-34)
    val UFU_UGA = DWE_VM_UFUGACTI2.alias("VM_UFUGACTI").join(DWE_VM_UGACTIVI2.alias("VM_UGACTIVI"), DWE_VM_UFUGACTI2("UGACT_ID") === DWE_VM_UGACTIVI2("UGACT_ID"), "left")
    val UFU_UGA_ELTRE = UFU_UGA.join(DWE_VM_ELTREPOB2, (DWE_VM_UFUGACTI2("DESDE_DT") <= DWE_VM_ELTREPOB2("HASTA_DT")) &&
      ((DWE_VM_UFUGACTI2.col("HASTA_DT").isNull && DWE_VM_ELTREPOB2("DESDE_DT") >= DWE_VM_ELTREPOB2.col("DESDE_DT")) ||
        (DWE_VM_UFUGACTI2.col("HASTA_DT").isNotNull && DWE_VM_UFUGACTI2.col("HASTA_DT") >= DWE_VM_ELTREPOB2.col("DESDE_DT"))), "left")
    val UFU_UGA_ELTRE_PROV = UFU_UGA_ELTRE.join(DWE_SGE_SAP_PROVEEDORES, DWE_SGE_SAP_PROVEEDORES("PROVE_ID") === DWE_VM_UFUGACTI("UNFAC_ID"), "right").show()

    //Parte con sparksql (líneas 35-47)
    val SPARKSQL1 = spark.sql(
      """  select (1 * coalesce(OP.PORCENTAJE_QT, 100) / 100) * coalesce(OU.PORCENTAJE_QT, 100) / 100 AS POBDC_QT,
        |                       OP.OPERADOR_ID OPERADOR_ID_OP, OU.OPERADOR_ID OPERADOR_ID_OU,
        |             COALESCE(OP.OPERADOR_ID, OU.OPERADOR_ID, 0) AS OPERADOR_ID,
        |             CASE WHEN OP.OPERADOR_ID IS NOT NULL THEN OP.PORCENTAJE_QT
        |                  ELSE OU.PORCENTAJE_QT
        |             END PORCENTAJE_QT,
        |             coalesce(OP.UTE_ID,0) AS UTE_ID,
        |             CASE WHEN OP.UTE_ID IS NOT NULL THEN OP.PORCENTAJE_QT
        |                  ELSE NULL
        |             END AS PORCENTAJE_UTE_QT ,OP.DESDE_DT,OP.HASTA_DT, OP.UFUGA_ID,OP.MEDIOSPP_SN
        |             from DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP OU  right join DWE_SGR_MU_ASIG_OPERADORES_UF_TMP OP on OP.UTE_ID = OU.UTE_ID
        |""".stripMargin
    )




}



  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilo


}
