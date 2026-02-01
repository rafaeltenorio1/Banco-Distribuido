import mysql.connector
import sys

class DBManager:
    def __init__(self, host, user, password, database=None, port=3306):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            # 'database': database  <-- Deixe comentado! Vamos conectar no servidor globalmente.
        }
        
        # Conexão persistente
        self.connection = None
        self.cursor = None
        
        print(f"[DB INIT] Tentando conectar ao MySQL em {host}...")
        self.conectar()

    def conectar(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            # buffered=True evita erro de "Unread result found"
            self.cursor = self.connection.cursor(dictionary=True, buffered=True)
            print(f"[DB CONN] Conexão estabelecida com sucesso (ID Conexão: {self.connection.connection_id})")
        except mysql.connector.Error as err:
            print(f"[DB CRITICAL] Falha ao conectar: {err}")
            sys.exit(1)

    def _get_current_db(self):
        """Helper para saber onde estamos conectados agora"""
        try:
            self.cursor.execute("SELECT DATABASE()")
            res = self.cursor.fetchone()
            if res:
                return list(res.values())[0]
        except:
            return "Unknown"
        return "None"

    def executar_query(self, sql):
        # 1. Verifica conexão
        if self.connection is None or not self.connection.is_connected():
            print("[DB WARN] Conexão perdida. Reconectando...")
            self.conectar()

        try:
            # 2. Log de Debug PRE-EXECUÇÃO
            db_atual = self._get_current_db()
            sql_clean = sql.strip().replace('\n', ' ')
            print(f"[DB EXEC] DB_ATUAL=[{db_atual}] | SQL: {sql_clean[:100]}...")

            # 3. Limpeza de buffer (casos raros de erro anterior)
            try: self.cursor.fetchall() 
            except: pass 

            # 4. Execução
            self.cursor.execute(sql)

            # 5. Tratamento SELECT vs ESCRITA
            if any(sql.strip().upper().startswith(cmd) for cmd in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]):
                resultado = self.cursor.fetchall()
                
                # Sanitização (converte datas/decimais para string)
                for row in resultado:
                    for key, value in row.items():
                        if value is not None and not isinstance(value, (int, float, str, bool, type(None))):
                            row[key] = str(value)
                
                print(f"   -> [DB RESULT] Retornou {len(resultado)} linhas.")
                return {"status": "OK", "dados": resultado}
                        
            else:
                self.connection.commit()
                print(f"   -> [DB COMMIT] Sucesso. Linhas afetadas: {self.cursor.rowcount}")
                return {"status": "OK", "mensagem": "Query executada com sucesso"}
                
        except mysql.connector.Error as err:
            print(f"   -> [DB ERROR] {err}")
            return {"status": "ERRO", "mensagem": str(err)}

    def get_full_dump(self):
        """
        Gera um dump completo de todos os bancos.
        Usa uma conexão separada para não poluir o estado da conexão principal.
        """
        dump = {}
        connector = None
        
        print("\n[DUMP START] --- Iniciando Varredura Completa ---")
        try:
            connector = mysql.connector.connect(**self.config)
            cursor = connector.cursor(dictionary=True, buffered=True)
            
            # Pega lista de bancos
            cursor.execute("SHOW DATABASES")
            all_dbs = [list(row.values())[0] for row in cursor.fetchall()]
            
            ignore_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']

            for db_name in all_dbs:
                if db_name in ignore_dbs: continue

                print(f"[DUMP SCAN] Analisando Banco: '{db_name}'")
                
                # Troca de contexto
                cursor.execute(f"USE {db_name}")
                
                # Lista tabelas
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                # Caso: Banco Vazio
                if not tables:
                    print(f"   -> [DUMP] Banco vazio encontrado.")
                    key = f"{db_name}.__EMPTY_DB__"
                    dump[key] = {
                        "database": db_name,
                        "table": None, "schema": None, "rows": []
                    }
                    continue

                # Caso: Banco com Tabelas
                table_names = [list(t.values())[0] for t in tables]
                print(f"   -> [DUMP] Tabelas encontradas: {table_names}")

                for table in table_names:
                    # Schema
                    cursor.execute(f"SHOW CREATE TABLE {table}")
                    row = cursor.fetchone()
                    create_sql = list(row.values())[1] if row else ""

                    # Dados
                    cursor.execute(f"SELECT * FROM {table}")
                    rows = cursor.fetchall()
                    
                    # Serialização
                    serialized_rows = []
                    for r in rows:
                        new_row = {}
                        for k, v in r.items():
                            if v is None: new_row[k] = None
                            elif isinstance(v, (int, float, bool)): new_row[k] = v
                            else: new_row[k] = str(v)
                        serialized_rows.append(new_row)
                    
                    print(f"      -> Tabela '{table}': {len(serialized_rows)} registros exportados.")
                    
                    key = f"{db_name}.{table}"
                    dump[key] = {
                        "database": db_name,
                        "table": table,
                        "schema": create_sql,
                        "rows": serialized_rows
                    }

            print("[DUMP END] --- Varredura Finalizada ---\n")
            return dump

        except Exception as e:
            print(f"[DUMP CRITICAL] Erro ao gerar dump: {e}")
            return {}
        finally:
            if connector and connector.is_connected():
                cursor.close()
                connector.close()
