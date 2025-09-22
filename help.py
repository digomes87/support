import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import array, col, expr, lit, struct, when
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)
from utils.logging_config import ETLLogger

def formata_df(resultado_df: DataFrame, numero_lote_payload_dmp: int, logger: ETLLogger) -> DataFrame:
    resultado_df = tratamento_df(resultado_df)
    resultado_df = agrupa_por_id_pessoa(resultado_df)
    resultado_df = agregacoes_campo(resultado_df)

    if resultado_df is None:
        logger.error("DataFrame esta none")
        return None
    resultado_df = organiza_estruturas_pos_agrupamento(resultado_df)

    resultado_df = compacta_em_lotes(resultado_df, numero_lote_payload_dmp)
    return resultado_df

def organiza_estruturas_pos_agrupamento(resultado_df: DataFrame) -> DataFrame:
    uuid_expr = F.expr("uuid()")
    resultado_df = resultado_df.select(
            struct(
                struct(
                    lit('Decidir análise de crédito PF - Consignado').alias("nom_fncd_serv_nego"),
                    uuid_expr.alias("cod_idef_prpt_sist_prod"),
                    uuid_expr.alias("cod_idef_tran_sist_cred"),
                    uuid_expr.alias("cod_idef_job_jorn"),
                    uuid_expr.alias("cod_idef_tran_sist_prod"),
                    lit('Batch').alias("cod_idef_prso_nego"),
                    col("cod_idef_pess").alias("cod_idef_pess"),
                    lit('').alias("nom_prso_sist_cred"),
                    lit('').alias("cod_stat_decs"),
                    lit('').alias("nom_tipo_decs_prpt"),
                    lit('').alias("nom_moto_anal_prpt"),
                    lit('F').alias("cod_tipo_pess"),
                    lit('').alias("nom_prod_cred"),
                    lit('').alias("nom_fase_cred"),
                    lit('').alias("nom_pilo_pltc_cred"),
                    lit('').alias("cod_vers_pltc"),
                    lit('').alias("cod_vers_tecn_pltc"),
                    lit('').alias("dat_hor_exeo_descs"),
                    ).alias("payloadIdentification"),
                struct(
                    lit('Batch').alias("tipoProcesso"),
                    struct(
                        lit(3180).alias("codCanal"),
                        lit(4532).alias("codsubCanal"),
                        col("numeroConvenio")
                    ).alias("solicitacao"),
                    col("dadosOferta"),
                    col("listaValoresCalculados"),
                    struct(
                        col("cod_idef_pess").alias("numeroDocumento"),
                        col("listaContratos"),
                        col("dadosEndividamento"),
                        col("dadosFatorRisco"),
                        col("indCorrentistaAntigo"),
                        col("dadosBacen"),
                        col("dadosPessoaFisica"),
                        col("dadosVisaoCliente"),
                        col("dadosModelos"),
                        col("dadosRenda")
                    ).alias("proponente")
                ).alias("payloadInput"),
                struct(
                    array().alias('auditSteps'),
                    array().alias('calculatedVars'),
                    lit(True).alias('isAuditEnabled'),
                    lit(True).alias('isCalculatedVarsEnabled'),
                ).alias('payloadAudit')
                ).alias('obj_dmp')
            )
    return resultado_df

def agrupa_por_id_pessoa(resultado_df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy('cod_idef_pess')
    resultado_df = resultado_df.withColumn(
                "valorTotalParcelaInterno",
                F.sum('Valor_Contratado_Interno').over(window_spec)
    )
    resultado_df = resultado_df.withColumn(
            "calculoDias",
            col("Situacao_BX").alias("calculoDias")
    )

    resultado_df = resultado_df.withColumn(
            "dias",
            when(col("calculoDias").isNull(), lit(-1))
            .otherwise(col("calculoDias")).cast(IntegerType())
    )
    return resultado_df

def agregacoes_campo(resultado_df: DataFrame) -> DataFrame:
    agregacoes = {
        "dadosBacen": F.struct(
           F.first('Modalidade_Operacao', True).alias("codSubModalidadeOperacao"),
           F.first('tipoInstituicao').alias('tipoInstituicao')
        ),
        "dadosModelos": F.struct(
            F.first('Cross_Consignado', True).alias("crossConsignado"),
            F.first('Cod_visao_Cliente', True).alias("visaoCliente"),
            F.first('Gene_Emprego').alias("geneEmprego")
        ),
        "dadosFatorRisco": F.struct(
            F.collect_set('Codigo_Exclusao_Comercial').alias("listaExclusao"),
            F.collect_set('Apontamento_BX').alias("listaApontamentos"),
            F.first('Gene_Emprego', True).alias("geneEmprego")
        ),
        "dadosPessoaFisica": F.struct(
            F.first('Idade', True).alias("idadeCliente"),
            F.first('Indicador_SPI', True).alias("spiVisaoConta"),
            F.first('Indicador_SPO', True).alias("spoVisaoConta"),
            F.collect_set('Vinculo').alias('listaVinculos'),
            F.collect_list('Cargos').alias('listaCargos')
        ),
        "dadosEndividamento": F.struct(
                F.first('Parcela_Mercado', True).alias('valorParcConsignado'),
                F.first('valorTotalParcelaInterno', True).alias('valorParcConsignadoItau')
        ),
        "dadosRenda": F.struct(
                F.first('Renda_Sispag', True).alias('rendaInternaCliente')
        ),
        "dadosOferta": F.struct(
                F.first('Claro_Nao', True).alias('flagInelebilidadeForte')
        ),
        "dadosVisaoCliente": F.struct(
                F.first('Target', True).alias('rating')
        ),
        "listaValoresCalculados": F.struct(
                F.first('pct_maxi_cpmm_rend').alias('percentual'),
                F.first('qtpzmax').alias('prazo'),
                F.first('Parcela_Elegivel_Refin').alias('vlrParcelaElegivel')
        ),
        "listaApontamentos": F.struct(
                F.first(F.when(F.col('Apontamento_BX').isNull(), -1).otherwise(F.col('Apontamento_BX').cast(IntegerType())), True).alias('codigo'),
                F.first(F.when(F.col('dias').isNull(), -1).otherwise(F.col('dias')), True).alias('recencia')
        ),
        "indCorrentistaAntigo": F.struct(
                F.first('indCorrentistaAntigo').alias('indCorrentistaAntigo')
        ),
        "listaContratos": F.collect_list(
                F.struct(
                    F.col('valorTotalParcelaInterno').alias('valorCnotrato')
                )
        ),
        "numeroConvenio": F.struct(
                F.first('Numero_Conveio').alias('numeroConvenio')
        )

    }

    resultado_agrupado = resultado_df.groupBy('cod_idef_pess').agg(
            *[agregacoes[key].alias(key) for key in agregacoes]
    )

    return resultado_agrupado


def tratamento_df(resultado_df: DataFrame) -> DataFrame:
    resultado_df = set_IndicadorSPI(resultado_df, 'Indicador_SPI')
    resultado_df = set_IndicadorSPO(resultado_df, 'Indicador_SPO')
    resultado_df = set_indicadorCNAO(resultado_df, 'Claro_Nao')
    resultado_df = calcula_idade(resultado_df)
    return resultado_df

def calcula_idade(resultado_df: DataFrame) -> DataFrame:
    resultado_df = resultado_df.withColumn('Idade', F.to_date(resultado_df['date'], 'yyyy-mm-dd'))
    resultado_df = resultado_df.withColumn('Idade', F.floor(F.datediff(F.current_date(), F.col('Idade')) / 356.25))
    return resultado_df

def set_IndicadorSPI(resultado_df: DataFrame, Indicador_SPI: str) -> DataFrame:
    resultado_df = resultado_df.withColumn(
        'Indicador_SPI',
        F.when(F.col('Indicador_SPI').isNull(), 'False').otherwise('True')
    )
    return resultado_df

def set_IndicadorSPO(resultado_df: DataFrame, Indicador_SPO: str) -> DataFrame:
    resultado_df = resultado_df.withColumn(
        'Indicador_SPO',
        F.when(F.col('Indicador_SPO').isNull(), 'False').otherwise('True')
    )
    return resultado_df

def set_indicadorCNAO(resultado_df: DataFrame, Claro_Nao: str) -> DataFrame:
    resultado_df = resultado_df.withColumn(
        'Claro_Nao',
        F.when(F.col('Claro_Nao') == 0, 'False').otherwise('True')
    )
    return resultado_df

def compacta_em_lotes(resultado_df: DataFrame, numero_lote_payload_dmp: int) -> DataFrame:
    new_schema = StructType([StructField('index', LongType(), False)] + resultado_df.schema.fields[:])
    resultado_df = (resultado_df.rdd.zipWithIndex().map(lambda x : (x[1],) + x[0]).toDF(schema=new_schema))
    resultado_df = resultado_df.withColumn('num_lote', ((F.col('index')/ numero_lote_payload_dmp).cast('int'))).drop('index')
    resultado_df = (resultado_df.groupBy('num_lote').agg(F.collect_list('obj_dmp').alias('fixed_array_of_Payload')).drop('num_lote'))
    return resultado_df
