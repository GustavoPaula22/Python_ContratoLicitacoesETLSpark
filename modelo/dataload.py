from modelo import configs as con
import pyodbc
from pyspark.sql import DataFrame

conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={
    con.server};Database={con.database};UID={con.user};PWD={con.senha}"


def sqlServer(df: DataFrame):
    result = execute_query(con.queryBusca)

    if result:
        data = [list(row) for row in result]
        if all(len(row) == 21 for row in data):
            spark = df.sparkSession
            dfTabela = spark.createDataFrame(data, schema=[
                "Apelido",
                "Demandante",
                "DataSolicitacao",
                "Modalidade",
                "CriterioJulgamento",
                "Escopo",
                "Objeto",
                "Programa",
                "DataEstimadaContratacao",
                "ProcessoSislog",
                "ProcessoAdministrativoSei",
                "DataAbertura",
                "ValorEstimado",
                "ValorContratado",
                "EmpresaVencedora",
                "StatusBI",
                "PorcentagemDesconto",
                "Prioridade",
                "UnidadeStatusBI",
                "DataEstimadaAcoesGelic",
                "AcoesGelic"
            ])

            df_merged = df.join(
                dfTabela, on='ProcessoAdministrativoSei', how='outer', indicator=True)
            dfResultado = df_merged.filter(df_merged['_merge'] != 'both').drop(
                '_merge',
                'Apelido_y',
                'Demandante_y',
                'DataSolicitacao_y',
                'Modalidade_y',
                'CriterioJulgamento_y',
                'Escopo_y',
                'Objeto_y',
                'Programa_y',
                'DataEstimadaContratacao_y',
                'ProcessoSislog_y',
                'DataAbertura_y',
                'ValorEstimado_y',
                'ValorContratado_y',
                'EmpresaVencedora_y',
                'StatusBI_y',
                'PorcentagemDesconto_y',
                'Prioridade_y',
                'UnidadeStatusBI_y',
                'DataEstimadaAcoesGelic_y',
                'AcoesGelic_y'
            )
            dfResultado = dfResultado.withColumnRenamed('_x', '')

            dfResultado.show(10, truncate=False)

            if dfResultado.count() > 0:
                try:
                    with pyodbc.connect(conn_str) as conn:
                        with conn.cursor() as cursor:
                            for row in dfResultado.collect():
                                cursor.execute(con.queryInsert, tuple(row))
                            conn.commit()
                            print("Processo Finalizado com Sucesso!")
                except pyodbc.Error as e:
                    print(f"Erro ao executar consulta: {e}")
            else:
                print("Dados já atualizados...")
        else:
            print("Erro: O número de colunas no resultado não corresponde ao esperado.")
    else:
        try:
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    for row in df.collect():
                        cursor.execute(con.queryInsert, tuple(row))
                    conn.commit()
                    print("Processo Finalizado com Sucesso!")
        except pyodbc.Error as e:
            print(f"Erro ao executar consulta: {e}")


def execute_query(query):
    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    return result
                else:
                    print("Nenhum dado retornado pela consulta.")
                    return None
    except pyodbc.Error as e:
        print(f"Erro ao executar consulta: {e}")
        return None
