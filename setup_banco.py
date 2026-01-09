import mysql.connector

# --- CONFIGURAÇÕES ---

def criar_banco():
    try:
        # 1. Conecta no MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="labsd",
            password= "labsd" 
        )
        cursor = conn.cursor()

        # 2. Executa os comandos SQL
        print("Criando banco de dados...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS ddb;")
        
        print("Selecionando o banco...")
        cursor.execute("USE ddb;")

        print("Criando tabela 'clientes'...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(100),
                email VARCHAR(100)
            );
        """)

        print("Inserindo dados de teste...")
        cursor.execute("INSERT INTO clientes (nome, email) VALUES ('Rafael', 'rafael@gmail.com');")
        
        conn.commit()
        print("✅ Sucesso! Banco de dados criado e populado.")

    except mysql.connector.Error as erro:
        print(f"❌ Erro ao conectar no MySQL: {erro}")
        print("Dica: Se der erro de 'Access denied', tente colocar 'root' ou 'mysql' como senha.")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    criar_banco()
