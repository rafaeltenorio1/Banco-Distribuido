import socket
import json
import random

# Configuração dos nós disponíveis para conexão
NODES = [
    {"ip": "localhost", "port": 5001},
    {"ip": "localhost", "port": 5002},
    {"ip": "localhost", "port": 5003}
]

def calcular_checksum(payload):
    dump = json.dumps(payload, sort_keys=True).encode()
    import hashlib
    return hashlib.md5(dump).hexdigest()

def enviar_query(sql):
    # BALANCEAMENTO DE CARGA: Escolhe um nó aleatório
    node = random.choice(NODES)
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((node['ip'], node['port']))
        
        payload = {"sql": sql}
        msg = {
            "tipo": "QUERY_REQ",
            "origem": "CLIENTE",
            "payload": payload,
            "checksum": calcular_checksum(payload)
        }
        
        s.send(json.dumps(msg).encode())
        
        # Aguarda resposta
        resp_raw = s.recv(4096).decode()
        response = json.loads(resp_raw)
        
        print("-" * 50)
        print(f"Conectado ao Nó: {node['port']}")
        print(f"Executado no Nó: {response.get('node_exec')}")
        print(f"Status: {response.get('result', {}).get('status')}")
        if 'dados' in response.get('result', {}):
            print("DADOS RETORNADOS:")
            for linha in response['result']['dados']:
                print(linha)
        print("-" * 50)
        s.close()
        
    except Exception as e:
        print(f"Erro ao conectar com o nó {node['port']}: {e}")

def menu():
    while True:
        print("\n=== CLIENTE DDB ===")
        print("1. Inserir Cliente (INSERT)")
        print("2. Listar Clientes (SELECT)")
        print("3. Query Personalizada")
        print("0. Sair")
        opt = input("Opção: ")
        
        if opt == '1':
            nome = input("Nome: ")
            email = input("Email: ")
            sql = f"INSERT INTO clientes (nome, email) VALUES ('{nome}', '{email}')"
            enviar_query(sql)
        elif opt == '2':
            enviar_query("SELECT * FROM clientes")
        elif opt == '3':
            sql = input("Digite a Query SQL: ")
            enviar_query(sql)
        elif opt == '0':
            break

if __name__ == "__main__":
    menu()