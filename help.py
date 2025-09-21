import copy
import re

import botocore


def update_table_data_catalog(date_partition, storage_description, logger, glue_client_role, database_name_spec, table_name_spec, lake_control_account_id, s3_client):
    logger.info(f'Criando particoes')
    s3_location = storage_description['Location']
    bucket_name = s3_location.split('/')[2]  # Extract bucket name from s3://bucket/path
    s3_prefix = '/'.join(s3_location.split('/')[3:])  # Extract prefix from s3://bucket/path

    limpa_particoes_existentes(database_name_spec, date_partition, glue_client_role, lake_control_account_id, table_name_spec)
    lista_num_lote = get_lista_num_lote_from_s3(bucket_name, f'{s3_prefix}/num_ano_mes_dia={date_partition}', s3_client)

    try:
        tamanho_lote = 200
        for i in range(0, len(lista_num_lote), tamanho_lote):
            list_lote_atual = lista_num_lote[i:i + tamanho_lote]

            partition_inputs = []
            for lote in list_lote_atual:
                custom_storage_description = copy.deepcopy(storage_description)

                custom_storage_description['Location'] = storage_description['Location'] + f"/num_ano_mes_dia={date_partition}/num_lote={lote}/"
                partition_inputs.append(
                        {
                            'Values': [
                                date_partition, lote
                            ],
                            "StorageDescriptor": custom_storage_description
                        }
                )
            glue_client_role.batch_create_partition(DatabaseName=database_name_spec, TableName=table_name_spec, CatalogId=lake_control_account_id, PartitionInputList=partition_inputs)
    except botocore.exceptions.ClientError as error:
        
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            logger.warning(f"Particao {date_partition} ja existe")
        
        if error.response['Error']['Code'] == 'AccessDeniedException':
            logger.warning(f"Erro de acesso a tabela: {table_name_spec}, sem permissao ou nao existe")

        if error.response['Error']['Code'] == 'ExpiredTokenException':
            logger.warning(f"Token de sessions AWS expirado, erro reproduzido ao tentar acessar {table_name_spec}")

        else:
            logger.error(f"Erro ao tentar gerar particao da tabela {table_name_spec}: {error}")
            raise error

def limpa_particoes_existentes(database_name_spec, date_partition, glue_client_role, lake_control_account_id, table_name_spec):
    lista_particoes = get_lista_particoes_spec_from_catalog(date_partition, glue_client_role, database_name_spec, table_name_spec, lake_control_account_id)

    if len(lista_particoes) != 0:
        delete_batch_spec_partitions(lista_particoes, glue_client_role, lake_control_account_id, database_name_spec, table_name_spec)

def lista_objetos_s3(bucket_name, prefixo, s3_client, continuation_token=None):
    if continuation_token:
        return s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefixo, Delimiter='/', ContinuationToken=continuation_token)
    else:
        return s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefixo, Delimiter='/')

def extrair_num_lote(pasta, exp_re):
    match_valor = re.search(exp_re, pasta['Prefix'])
    if match_valor:
        return match_valor.group(1)
    return None

def get_lista_num_lote_from_s3(bucket_name, prefixo, s3_client):
    pastas = set()
    continua = True
    continua_token = None
    exp_re = r'num_lote=(\d+)'

    while continua:
        resultado = lista_objetos_s3(bucket_name, prefixo, s3_client, continua_token)

        if 'CommonPrefixes' in resultado:
            for pasta in resultado['CommonPrefixes']:
                num_lote = extrair_num_lote(pasta, exp_re)
                if num_lote:
                    pastas.add(num_lote)
        continua_token = resultado.get('NextContinuationToken')
        continua = continua_token is not None

    list_num_lotes = sorted(pastas, key=int) if pastas else []

    return list_num_lotes

def get_lista_particoes_spec_from_catalog(num_ano_mes_dia, glue_client_role, database_name_spec, table_name_spec, lake_control_account_id):
    list_particoes = []

    partition_info = glue_client_role.get_partitions(
            DatabaseName=database_name_spec,
            TableName=table_name_spec,
            ExcludeColumnSchema=True,
            Expression=f"num_ano_mes_dia={num_ano_mes_dia}",
            CatalogId=lake_control_account_id
    )

    while True:
        if partition_info.get('Partitions'):
            for partition in partition_info['Partitions']:
                if len(partition['Values']) != 0:
                    list_particoes.append({'Values': partition['Values']})

        next_token = partition_info.get('NextToken')
        if not next_token:
            break

        partition_info = glue_client_role.get_partitions(
                DatabaseName=database_name_spec,
                TableName=table_name_spec,
                ExcludeColumnSchema=True,
                NextToken=next_token,
                Expression=f"num_ano_mes_dia={num_ano_mes_dia}",
                CatalogId=lake_control_account_id
        )

    return list_particoes

def delete_batch_spec_partitions(lista_particoes, glue_client_role, lake_control_account_id, database_name_spec, table_name_spec):
    tamanho_lote = 30
    for i in range(0, len(lista_particoes), tamanho_lote):
        lote_atual = lista_particoes[i:i + tamanho_lote]
        glue_client_role.batch_delete_partition(
            CatalogId=lake_control_account_id,
            DatabaseName=database_name_spec,
            TableName=table_name_spec,
            PartitionsToDelete=lote_atual
        )


