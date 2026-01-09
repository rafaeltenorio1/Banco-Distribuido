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
        connector = None
        try:
            connector = mysql.connector.connect(**self.config)
            cursor = connector.cursor(dictionary=True)
            
            # Se for SELECT
            if sql.strip().upper().startswith("SELECT"):
                cursor.execute(sql)
                resultado = cursor.fetchall()
                return {"status": "OK", "dados": resultado}
            
            # Se for INSERT/UPDATE/DELETE
            else:
                cursor.execute(sql)
                connector.commit()
                return {"status": "OK", "mensagem": "Operação realizada com sucesso"}
                
        except mysql.connector.Error as err:
            return {"status": "ERRO", "mensagem": str(err)}
        finally:
            if connector and connector.is_connected():
                cursor.close()
                connector.close()
