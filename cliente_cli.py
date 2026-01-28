import socket
import hashlib
import json
import random

# Configuração: Mesmos nós que o Middleware conhece
NODES = [
    {"ip": "localhost", "porta": 5001},
    # Adicione outros IPs de nós aqui se houver
]

def calcular_checksum(payload):
    """Gera o MD5 da string (no middleware você usa a string da query)"""
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def enviar_query_cli():
    print("=== Cupuaçu DB CLI Client ===")
    print("Digite 'sair' para encerrar.\n")

    while True:
        sql = input("SQL query > ").strip()
        
        if sql.lower() in ['sair', 'exit', 'quit']:
            break
        
        if not sql:
            continue

        # Seleciona um nó aleatório para enviar a requisição
        node = random.choice(NODES)
        
        try:
            # Criar socket
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(5)
            client.connect((node['ip'], node['porta']))

            # Protocolo do Middleware: TIPO \x1f SQL \x1f CHECKSUM
            tipo = "QUERY_REQ"
            checksum = calcular_checksum(sql)
            mensagem = f"{tipo}\x1f{sql}\x1f{checksum}"
            print(sql, checksum)
            client.send(mensagem.encode("utf-8"))

            # Receber resposta
            # O Middleware responde no formato: ID_NODE \x1f RESULTADO_JSON \x1f CHECKSUM
            resp_raw = client.recv(409600).decode("utf-8")
            
            if resp_raw:
                partes = resp_raw.split("\x1f")
                if len(partes) >= 2:
                    node_id = partes[0]
                    resultado = partes[1]
                    
                    print(f"\n[Resposta do Nó {node_id}]")
                    # Tenta formatar o JSON se vier como lista/dict
                    try:
                        dados = json.loads(resultado.replace("'", '"')) # Fix simples para aspas
                        print(json.dumps(dados, indent=4))
                    except:
                        print(resultado)
                else:
                    print(f"Resposta malformada: {resp_raw}")
            
            client.close()
            print("-" * 30)

        except Exception as e:
            print(f"[ERRO] Falha ao conectar em {node['ip']}: {e}")

if __name__ == "__main__":
    enviar_query_cli()
