import uuid
from logging import Logger

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (col, collect_list, current_date,
                                   date_format, expr, lit, struct, to_date,
                                   udf, when)
from pyspark.sql.types import IntegerType, StringType

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

def safe_coalesce(value, dtype, logger: Logger):
    """Função de coalesce segura para tratar valores nulos"""
    if value is not None and value != '':
        return value
    else:
        if dtype == "string":
            return -1
        elif dtype == "double":
            return -1.0
        elif dtype == "boolean":
            return False
        elif dtype == "array":
            return [-1]
        elif dtype == "struct":
            return {"default": "-1"}
        else:
            return -1

def get_uuid_expr():
    """Gera expressão UUID"""
    return F.expr("uuid()")

def formata_payload_dmp(resultado_df: DataFrame, numero_lote_payload_dmp: int, logger: Logger) -> DataFrame:
    """Função principal para formatar DataFrame para payload DMP"""
    logger.info("Iniciando formatação do DataFrame para payload DMP")
    
    resultado_df = tratamento_df(resultado_df, logger)
    resultado_df = agrupa_por_id_pessoa(resultado_df)
    resultado_df = agregacoes_campo(resultado_df)

    if resultado_df is None:
        logger.error("DataFrame é None")
        return None
        
    resultado_df = organiza_estruturas_pos_agrupamento(resultado_df)
    resultado_df = compacta_em_lotes(resultado_df, numero_lote_payload_dmp)
    return resultado_df

def tratamento_df(resultado_df: DataFrame, logger: Logger) -> DataFrame:
    """Tratamento básico do DataFrame"""
    logger.info("Iniciando tratamento básico do DataFrame")
    
    # Adiciona colunas básicas com valores padrão
    resultado_df = resultado_df.withColumn('rdgHash', lit(-1))
    resultado_df = resultado_df.withColumn('isAuditEnabled', lit(True))
    resultado_df = resultado_df.withColumn('isCalculatedVarsEnabled', lit(True))
    
    # Formatação de data
    resultado_df = resultado_df.withColumn(
        "dataBeneficio",
        when(col("num_dat_incu_benf").isNull() | (col("num_dat_incu_benf") == ''), lit("0000-00-00"))
        .otherwise(date_format(to_date(col("num_dat_incu_benf"), "dd.MM.yyyy"), "yyyy-MM-dd"))
    )
    
    # Cálculo de idade
    resultado_df = resultado_df.withColumn(
        "dataAtual",
        current_date()
    )
    
    resultado_df = resultado_df.withColumn(
        "calculoIdade",
        expr("datediff(dataAtual, to_date(num_dat_incu_benf, 'dd.MM.yyyy')) / 365.25")
    )
    
    resultado_df = resultado_df.withColumn(
        "idadeInt",
        col("calculoIdade").cast(IntegerType())
    )
    
    return resultado_df

def agrupa_por_id_pessoa(resultado_df: DataFrame) -> DataFrame:
    """Agrupa DataFrame por ID da pessoa com funções de janela"""
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
    """Agregações de campo com lógica de negócio específica"""
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
                F.first('CorrentistaAntigo').alias('indCorrentistaAntigo')
        ),
        "listaContratos": F.collect_list(
                F.struct(
                    F.col('valorTotalParcelaInterno').alias('valorContrato')
                )
        ),
        "numeroConvenio": F.struct(
                F.first('Numero_Convenio').alias('numeroConvenio')
        )
    }

    resultado_agrupado = resultado_df.groupBy('cod_idef_pess').agg(
            *[agregacoes[key].alias(key) for key in agregacoes]
    )

    return resultado_agrupado

def organiza_estruturas_pos_agrupamento(resultado_df: DataFrame) -> DataFrame:
    """Organiza estruturas após agrupamento com estrutura JSON completa"""
    uuid_expr = get_uuid_expr()
    
    return resultado_df.select(
        struct(
            struct(
                lit('Decidir análise de crédito PF - Consignado').alias("nom_fncd_serv_nego"),
                uuid_expr.alias("cod_idef_prpt_sist_prod"),
                uuid_expr.alias("cod_idef_tran_sist_cred"),
                lit('Batch').alias("cod_idef_prso_nego"),
                col("cod_idef_pess").alias("cod_idef_pess"),
                lit('F').alias("cod_tipo_pess")
            ).alias("payloadIdentification"),
            struct(
                col("dadosOferta").alias("dadosOferta"),
                lit(0).alias("digitoAleatorio"),
                col("listaContratos").alias("listaContratos"),
                col("dadosEndividamento").alias("dadosEndividamento"),
                col("dadosFatorRisco").alias("dadosFatorRisco"),
                col("dadosBacen").alias("dadosBacen"),
                col("dadosModelos").alias("dadosModelos"),
                col("dadosPessoaFisica").alias("dadosPessoaFisica"),
                col("dadosRenda").alias("dadosRenda"),
                col("dadosVisaoCliente").alias("dadosVisaoCliente"),
                col("listaValoresCalculados").alias("listaValoresCalculados"),
                col("listaApontamentos").alias("listaApontamentos"),
                col("indCorrentistaAntigo").alias("indCorrentistaAntigo"),
                col("numeroConvenio").alias("numeroConvenio")
            ).alias("payloadInput")
        ).alias('obj_dmp')
    )

def compacta_em_lotes(resultado_df: DataFrame, numero_lote_payload_dmp: int) -> DataFrame:
    """Compacta DataFrame em lotes"""
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    
    resultado_df = resultado_df.withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
    
    resultado_df = resultado_df.withColumn(
        "batch_number",
        ((col("row_number") - 1) / numero_lote_payload_dmp).cast(IntegerType())
    )
    
    return resultado_df.groupBy("batch_number").agg(
        collect_list(col("obj_dmp")).alias("batch_data")
    )

def set_IndicadorSPI(resultado_df: DataFrame) -> DataFrame:
    """Define indicador SPI baseado em regras de negócio"""
    return resultado_df.withColumn(
        "Indicador_SPI",
        when(col("Indicador_SPI").isNull(), lit("N"))
        .otherwise(col("Indicador_SPI"))
    )

def set_IndicadorSPO(resultado_df: DataFrame) -> DataFrame:
    """Define indicador SPO baseado em regras de negócio"""
    return resultado_df.withColumn(
        "Indicador_SPO",
        when(col("Indicador_SPO").isNull(), lit("N"))
        .otherwise(col("Indicador_SPO"))
    )

def set_indicadorCNAO(resultado_df: DataFrame) -> DataFrame:
    """Define indicador CNAO baseado em regras de negócio"""
    return resultado_df.withColumn(
        "Claro_Nao",
        when(col("Claro_Nao").isNull(), lit("N"))
        .otherwise(col("Claro_Nao"))
    )

def calcula_idade(resultado_df: DataFrame) -> DataFrame:
    """Calcula idade a partir da data de nascimento"""
    return resultado_df.withColumn(
        "Idade",
        F.floor(F.datediff(F.current_date(), F.to_date(F.col("data_nascimento"), "yyyy-MM-dd")) / 365.25)
    )
