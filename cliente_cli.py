import socket
import hashlib
import ast
import random

# Configuração: Mesmos nós que o Middleware conhece
NODES = [
    {"ip": "localhost", "porta": 5001},
    {"ip": "localhost", "porta": 5002},
    {"ip": "localhost", "porta": 5003},
]

def calcular_checksum(payload):
    
    #Gera o MD5 da string (no middleware você usa a string da query)
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
            mensagem = f"{tipo}\x1f{sql}\x1f{checksum}\x1fCLI"
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
                        # Tenta converter a string Python de volta para Lista/Dict
                        dados = ast.literal_eval(resultado)
                        
                        # Imprime bonito
                        if isinstance(dados, list):
                            for linha in dados:
                                print(linha)
                        else:
                            print(dados)
                    except:
                            # Se não der pra converter, imprime o texto puro
                        print(resultado)
                else:
                    print(f"Resposta malformada: {resp_raw}")
            
            client.close()
            print("-" * 30)

        except Exception as e:
            print(f"[ERRO] Falha ao conectar em {node['ip']}: {e}")

if __name__ == "__main__":
    enviar_query_cli()
