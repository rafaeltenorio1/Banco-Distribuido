import mysql.connector

def resetar_banco():
    connector = None
    try:
        # Conecta sem especificar o banco de dados, pois vamos apagá-lo
        connector = mysql.connector.connect(
            host="localhost",
            user="labsd",
            password="labsd",
            port=3306 # ou 3307, dependendo da sua configuração
        )
        cursor = connector.cursor()

        print("[RESET] Apagando banco de dados antigo.")
        cursor.execute("DROP DATABASE IF EXISTS ddb;")
        
        print("[RESET] Criando banco de dados novo...")
        cursor.execute("CREATE DATABASE ddb;")
        cursor.execute("USE ddb;")

        print("[RESET] Criando tabelas...")
        # Recria a tabela usuarios
        cursor.execute("""
            CREATE TABLE Usuarios (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(100),
                email VARCHAR(100)
            );
        """)
        
        # Se tiver outras tabelas (ex: produtos, log), adicione os CREATE aqui
        # cursor.execute("CREATE TABLE ...")

        connector.commit()
        print("[SUCESSO] Banco de dados zerado e pronto para uso.")

    except mysql.connector.Error as erro:
        print(f"[ERRO] Falha ao resetar banco: {erro}")
    
    finally:
        if connector and connector.is_connected():
            cursor.close()
            connector.close()

if __name__ == "__main__":
    resetar_banco()