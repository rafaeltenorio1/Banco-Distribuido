import mysql.connector


def criar_banco():
    try:
        connector = mysql.connector.connect(
            host="localhost",
            user="labsd",
            password= "labsd" 
        )
        cursor = connector.cursor()

        print("[INFO] Criando banco de dados")
        cursor.execute("CREATE DATABASE IF NOT EXISTS ddb;")
        
        cursor.execute("USE ddb;")

        print("[INFO] Criando tabela 'clientes'")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(100),
                email VARCHAR(100)
            );
        """)

        
        connector.commit()
        print("[INFO] Banco de dados criado.")

    except mysql.connector.Error as erro:
        print(f"[ERROR] Erro ao conectar no MySQL: {erro}")
    
    finally:
        if 'conn' in locals() and connector.is_connected():
            cursor.close()
            connector.close()

if __name__ == "__main__":
    criar_banco()
