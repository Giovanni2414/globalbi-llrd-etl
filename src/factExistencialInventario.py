import json
import pandas as pd
from datetime import datetime
from utils.database_manager import DatabaseManager
import logging

def limpiar_saldos(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla FactExistenciaInventarios")
    try:
        query = 'TRUNCATE TABLE compras.FactExistenciaInventarios'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo limpiar saltos: {e}")

def cargue_existencia_inventarios(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando existencia inventarios")
    query = """
        SELECT [f400_id_cia] IdCompania ,
            [f400_id_instalacion] IdInstalacion ,
            [f400_rowid_item_ext] IdItemExt ,
            f150_id codbodega ,
            f150_descripcion descbodega ,
            f120_referencia IdReferencia ,
            [f400_costo_prom_uni] CostoPromUnit ,
            [f400_costo_prom_tot] CostoPromTot ,
            [f400_cant_existencia_1] CantExistencia ,
            [f400_cant_existencia_2] CantExistencia2 ,
            [f400_consumo_promedio] ConsumoPromedio
        FROM [UnoEE_Lloreda_Real].[dbo].[t400_cm_existencia]
        INNER JOIN t150_mc_bodegas ON f150_rowid = f400_rowid_bodega
        INNER JOIN t121_mc_items_extensiones ON f121_rowid = f400_rowid_item_ext
        INNER JOIN t120_mc_items ON f120_rowid = f121_rowid_item
        WHERE f400_id_cia = 2
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FechaCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FechaModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania_Int'] = df['IdCompania'].astype('int32')

    query = """
        SELECT IdOrganizacion, IdCompania AS IdCompania_Int FROM siesa.tbOrganizacionCompanias 
    """
    df_org = lloreda_manager.execute_query_get_pandas(query)
    merged_df = df.merge(df_org, on='IdCompania_Int', how='inner')

    query = """
    INSERT INTO compras.FactExistenciaInventarios (IdCompania, IdOrganizacion, IdInstalacion,
    CodBodega, Bodega, IdItemExtension, CodReferencia, CostoPromUni, CostoPromTot,
    CantExistencia1, CantExistencia2, Consumo_promedio) VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, merged_df[['IdCompania_Int','IdOrganizacion',
                                                              'IdInstalacion','codbodega',
                                                              'descbodega','IdItemExt','IdReferencia',
                                                              'CostoPromUnit','CostoPromTot',
                                                              'CantExistencia','CantExistencia2',
                                                              'ConsumoPromedio']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue existencia inventarios: {e}")

def ejecutar_fact_existencia():
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='errors.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(levelname)s [%(filename)s:%(funcName)s] %(message)s')
    logger.info('Iniciando cargue fact existencial inventario')

    with open('../config/database_credentials.json') as f:
        config_file = json.load(f)
    database_name = 'destino'
    lloreda_manager = DatabaseManager(config_file, database_name, use_pooling=False)
    lloreda_manager.connect()

    with open('../config/database_credentials.json') as f:
        config_file = json.load(f)
    database_name = 'origen'
    siesa_manager = DatabaseManager(config_file, database_name, use_pooling=False)
    siesa_manager.connect()

    # Operaciones principales
    limpiar_saldos(lloreda_manager, logger)
    cargue_existencia_inventarios(lloreda_manager, siesa_manager, logger)
    ############################

    lloreda_manager.disconnect()
    siesa_manager.disconnect()

if __name__ == "__main__":
    ejecutar_fact_existencia()