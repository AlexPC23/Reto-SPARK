package Recogida.Common

import Recogida.spark
import org.apache.spark.sql.functions.{col, isnull, lit, to_timestamp, when}
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
    val DWE_SGR_MU_ASIG_OPERADORES_UF_TMP = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGR_MU_ASIG_OPERADORES_UF_TMP_*")
    val DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP_*")
    val DWE_SGE_SAP_PROVEEDORES = spark.read.option("inferSchema", true).option("header", true).csv("DWE_SGE_SAP_PROVEEDORES_*")

    //DWE_VM_UAACTIVI.show()
    //DWE_VM_UGACTMUN.show()
    DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP.createTempView("DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP")
    DWE_SGR_MU_ASIG_OPERADORES_UF_TMP.createTempView("DWE_SGR_MU_ASIG_OPERADORES_UF_TMP")

    //Filtramos (lt = less than, gt = greater than, || = or)

    //Primero probamos de una en una
    //val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30")))).show()
    //val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-xx-xx)) && DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("HASTA_DT").gt("2017-xx-xx))
    //val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter(DWE_VM_TIPOLFAC("HASTA_DT").isNull).show()

    //Filters
    val DWE_VM_TIPOLFAC2 = DWE_VM_TIPOLFAC.filter((DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_TIPOLFAC("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_TIPOLFAC("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_TIPOLFAC("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_TIPOLFAC("HASTA_DT").isNull))) //TP
    val DWE_VM_UAACTIVI2 = DWE_VM_UAACTIVI.filter((DWE_VM_UAACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UAACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UAACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UAACTIVI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UAACTIVI("HASTA_DT").isNull))) //U
    val DWE_VM_UGACTMUN2 = DWE_VM_UGACTMUN.filter((DWE_VM_UGACTMUN("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UGACTMUN("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UGACTMUN("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UGACTMUN("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UGACTMUN("HASTA_DT").isNull))) //M
    val DWE_VM_UFTRGMUN2 = DWE_VM_UFTRGMUN.filter((DWE_VM_UFTRGMUN("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UFTRGMUN("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UFTRGMUN("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UFTRGMUN("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UFTRGMUN("HASTA_DT").isNull))) //T
    val DWE_VM_ENTLTPRE2 = DWE_VM_ENTLTPRE.filter((DWE_VM_ENTLTPRE("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_ENTLTPRE("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_ENTLTPRE("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_ENTLTPRE("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_ENTLTPRE("HASTA_DT").isNull))) //R
    val DWE_VM_ELTREPOB2 = DWE_VM_ELTREPOB.filter((DWE_VM_ELTREPOB("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_ELTREPOB("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_ELTREPOB("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_ELTREPOB("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_ELTREPOB("HASTA_DT").isNull))) //E
    val DWE_VM_UGACTIVI2 = DWE_VM_UGACTIVI.filter((DWE_VM_UGACTIVI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UGACTIVI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UGACTIVI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UGACTIVI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UGACTIVI("HASTA_DT").isNull))) //G
    val DWE_VM_UFUGACTI2 = DWE_VM_UFUGACTI.filter((DWE_VM_UFUGACTI("DESDE_DT").lt(lit("2017-08-01")) && (DWE_VM_UFUGACTI("DESDE_DT").gt(lit("2017-06-30")))) || (DWE_VM_UFUGACTI("DESDE_DT").lt(lit("2017-07-01")) && (DWE_VM_UFUGACTI("HASTA_DT").gt(lit("2017-07-01")) || DWE_VM_UFUGACTI("HASTA_DT").isNull))) //UF
    //val DWE_VM_TIPOLENT2 = DWE_VM_TIPOLENT.filter((DWE_VM_TIPOLENT("DESDE_DT").lt(lit(DWE_VM_ELTREPOB("DESDE_DT"))) && (DWE_VM_TIPOLENT("HASTA_DT").gt(lit(DWE_VM_ELTREPOB("HASTA_DT"))) || (DWE_VM_TIPOLENT("HASTA_DT").isNull)))) //S


    //Hacemos los joins (líneas 20 y 27-31)
    DWE_VM_UFUGACTI2.join(DWE_SGE_SAP_PROVEEDORES, DWE_VM_UFUGACTI2("UNFAC_ID") === DWE_SGE_SAP_PROVEEDORES("PROVE_ID"), "right")
    DWE_VM_TIPOLFAC2.join(DWE_VM_ENTLTPRE, DWE_VM_TIPOLFAC2("ELMUN_ID") === DWE_VM_ENTLTPRE2("ELMUN_ID"), "right")



    //Triple join (líneas 32-34)
    val UFU_UGA = DWE_VM_UFUGACTI2.alias("VM_UFUGACTI").join(DWE_VM_UGACTIVI2.alias("VM_UGACTIVI"), DWE_VM_UFUGACTI2("UGACT_ID") === DWE_VM_UGACTIVI2("UGACT_ID"), "left")
    val UFU_UGA_ELTRE = UFU_UGA.join(DWE_VM_ELTREPOB2, (DWE_VM_UFUGACTI2("DESDE_DT") <= DWE_VM_ELTREPOB2("HASTA_DT")) &&
      ((DWE_VM_UFUGACTI2.col("HASTA_DT").isNull && DWE_VM_ELTREPOB2("DESDE_DT") >= DWE_VM_ELTREPOB2.col("DESDE_DT")) ||
        (DWE_VM_UFUGACTI2.col("HASTA_DT").isNotNull && DWE_VM_UFUGACTI2.col("HASTA_DT") >= DWE_VM_ELTREPOB2.col("DESDE_DT"))), "left")
    val UFU_UGA_ELTRE_PROV = UFU_UGA_ELTRE.join(DWE_SGE_SAP_PROVEEDORES, DWE_SGE_SAP_PROVEEDORES("PROVE_ID") === DWE_VM_UFUGACTI("UNFAC_ID"), "right")

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
        |             from DWE_SGR_MU_ASIG_OPERADORES_UTE_TMP OU right join DWE_SGR_MU_ASIG_OPERADORES_UF_TMP OP on OP.UTE_ID = OU.UTE_ID""".stripMargin)

    val JOINFINAL = SPARKSQL1.alias("OP").join(DWE_VM_UFUGACTI2, DWE_VM_UFUGACTI2("UFUGA_ID") === SPARKSQL1("UFUGA_ID"), "left")

    //Joins (línea 48)
    //Alias

    val JOINDATOS = DWE_VM_UAACTIVI2.alias("U")
      .join(DWE_VM_UGACTIVI2.alias("G"), col("U.UAACT_ID") === col("G.UAACT_ID"), "inner")
      .join(DWE_VM_UGACTMUN2.alias("M"), col("G.UGACT_ID") === col("M.UGACT_ID"), "inner")
      .join(DWE_VM_ENTLOCAL.alias("L"), col("M.ELMUN_ID") === col("L.ELMUN_ID"), "inner")
      .join(DWE_VM_UFUGACTI2.alias("F"), col("G.UGACT_ID") === col("F.UGACT_ID"), "inner")
      .join(DWE_VM_UFTRGMUN2.alias("T"), col("F.UFUGA_ID") === col("T.UFUGA_ID"), "inner")
      .select("U.UNADM_ID", "U.ACTIV_ID", "G.UNGES_ID", "L.ELMUN_ID", "T.MUNTR_ID", "F.UNFAC_ID", "T.UFTRG_ID", "F.UFUGA_ID")

    val JOINDATOS2 = JOINDATOS.alias("total")
      .join(DWE_VM_ENTLTPRE2.alias("R"), JOINDATOS("ELMUN_ID") === DWE_VM_ENTLTPRE2("ELMUN_ID") && JOINDATOS("MUNTR_ID") === DWE_VM_ENTLTPRE2("MUNTR_ID"), "inner")
      .join(DWE_VM_ELTREPOB2.alias("E"), DWE_VM_ELTREPOB2("UFTRG_ID") === JOINDATOS("UFTRG_ID"), "inner")
      .join(DWE_VM_POBPERST.alias("P"), DWE_VM_POBPERST("UFUGA_ID") === JOINDATOS("UFUGA_ID"), "inner")
      .join(DWE_VM_UNIDADMI.alias("UA"), DWE_VM_UNIDADMI("UNADM_ID") === JOINDATOS("UNADM_ID"), "inner")
      .join(DWE_VM_COMUAUTO.alias("C"), DWE_VM_COMUAUTO("COMAU_ID") === DWE_VM_UNIDADMI("COMAU_ID"), "inner")
      .join(DWE_VM_TIPOLENT.alias("S"), DWE_VM_TIPOLENT("ELMUN_ID") === JOINDATOS("ELMUN_ID"), "inner")
      .join(DWE_VM_TPRECOGI.alias("TP"), DWE_VM_TPRECOGI("TPREC_ID") === DWE_VM_ENTLTPRE2("TPREC_ID"), "inner")
      .select("total.UNADM_ID", "total.ACTIV_ID", "total.UNGES_ID", "total.ELMUN_ID", "total.UFTRG_ID", "total.UFUGA_ID","total.UNFAC_ID", "E.DESDE_DT", "R.TPREC_ID", "S.TPENT_ID", "TP.PROCE_ID", "E.POBIN_QT", "E.POBLA_QT", "E.VERSI_ID") //"R.TPGFA_ID"
      .join(UFU_UGA_ELTRE_PROV.alias("UF2"), UFU_UGA_ELTRE_PROV("UFUGA_ID") === JOINDATOS("UFUGA_ID"), "inner")
      .join(JOINFINAL.alias("JOINFINAL"), JOINFINAL("OP.UFUGA_ID") === JOINDATOS("UFUGA_ID"), "inner")
      .select("E.DESDE_DT", "total.UNADM_ID", "total.ACTIV_ID", "total.UNGES_ID", "total.ELMUN_ID", "total.UFTRG_ID", "UF2.UFUGA_ID","total.UNFAC_ID", "R.TPREC_ID", "S.TPENT_ID", "TP.PROCE_ID", "E.POBIN_QT", "E.POBLA_QT", "E.VERSI_ID", "JOINFINAL.OPERADOR_ID", "UF2.PROVE_NM", "JOINFINAL.POBDC_QT", "JOINFINAL.PORCENTAJE_QT", "JOINFINAL.UTE_ID", "JOINFINAL.PORCENTAJE_UTE_QT", "JOINFINAL.MEDIOSPP_SN") //R.TPGFA_ID"

    //Líneas (10-13)
    var df = JOINDATOS2.withColumn("POBDC_QT", when(col("POBDC_QT").isNull, 0).otherwise(col("POBDC_QT")))
      .withColumn("PROCE_ID", when(col("PROCE_ID").isNull, 0).otherwise(col("PROCE_ID")))
      .withColumn("OPERADOR_ID", when(col("OPERADOR_ID").isNull, 0).otherwise(col("OPERADOR_ID")))
      .withColumn("POBDC_QT", when(col("POBDC_QT") === 0, 1).otherwise(col("POBDC_QT")))

      .withColumn("POBGC_QT", col("POBLA_QT") * col("POBDC_QT")) //POBGC_QT
      .withColumn("POBDC_QT", col("POBIN_QT") * col("POBDC_QT")) //POBDC_QT.
      .show()

      //.select("DESDE_DT", "UNADM_ID", "ACTIV_ID", "UNGES_ID", "ELMUN_ID", "UFTRG_ID", "UNFAC_ID", "TPREC_ID", "TPENT_ID", "PROCE_ID","UFUGA_ID", "PROVE_NM", "OPERADOR_ID", "POBDC_QT","POBGC_QT", "E.VERSI_ID").show()

  }





  def CargaMedios(): Unit = {
  }

  def CargaKilos():Unit={}
  //creacion de la tabla kilo


}
