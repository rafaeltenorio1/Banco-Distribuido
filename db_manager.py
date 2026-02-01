import mysql.connector

class DBManager:
    def __init__(self, host, user, password, database, port=3306):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }

    def executar_query(self, sql):
        connector = None
        try:
            connector = mysql.connector.connect(**self.config)
            cursor = connector.cursor(dictionary=True, buffered=True)
            
            # Se for SELECT
            if any(sql.strip().upper().startswith(cmd) for cmd in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]):
                cursor.execute(sql)
                resultado = cursor.fetchall()

                for row in resultado:
                    for key, value in row.items():
                        if value is not None and not isinstance(value, (int, float, str, bool, type(None))):
                            row[key] = str(value)
                return {"status": "OK", "dados": resultado}
                        
            else:
                cursor.execute(sql)
                connector.commit()
                return {"status": "OK", "mensagem": "Query executada com sucesso"}
                
        except mysql.connector.Error as err:
            return {"status": "ERRO", "mensagem": str(err)}
        finally:
            if connector and connector.is_connected():
                cursor.close()
                connector.close()

    def get_full_dump(self):
        dump = {}
        connector = None
        try:
            connector = mysql.connector.connect(**self.config)
            cursor = connector.cursor(dictionary=True, buffered=True)

            # 1. Pega lista de todas as tabelas
            cursor.execute("SHOW TABLES")
            # O resultado vem como [{'Tables_in_ddb': 'clientes'}, ...]
            tables = [list(row.values())[0] for row in cursor.fetchall()]

            for table in tables:
                table_data = {"schema": "", "rows": []}

                # 2. Pega o comando de criação (Schema)
                cursor.execute(f"SHOW CREATE TABLE {table}")
                row = cursor.fetchone()
                if row:
                    # Geralmente a chave é 'Create Table'
                    table_data["schema"] = row.get("Create Table")

                # 3. Pega os dados
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()
                
                # Serializa dados para JSON (converte datas/decimais para string)
                serialized_rows = []
                for r in rows:
                    new_row = {}
                    for k, v in r.items():
                        if v is None:
                            new_row[k] = None
                        elif isinstance(v, (int, float, bool)):
                            new_row[k] = v
                        else:
                            new_row[k] = str(v)
                    serialized_rows.append(new_row)
                
                table_data["rows"] = serialized_rows
                dump[table] = table_data

            return dump

        except Exception as e:
            print(f"[ERRO DUMP] {e}")
            return {}
        finally:
            if connector and connector.is_connected():
                cursor.close()
                connector.close()