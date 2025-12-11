import mysql.connector

class DBManager:
    def __init__(self, host, user, password, database):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }

    def executar_query(self, sql):
        conn = None
        try:
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor(dictionary=True)
            
            # Se for SELECT
            if sql.strip().upper().startswith("SELECT"):
                cursor.execute(sql)
                result = cursor.fetchall()
                return {"status": "OK", "dados": result}
            
            # Se for INSERT/UPDATE/DELETE
            else:
                cursor.execute(sql)
                conn.commit()
                return {"status": "OK", "mensagem": "Operação realizada com sucesso"}
                
        except mysql.connector.Error as err:
            return {"status": "ERRO", "mensagem": str(err)}
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()