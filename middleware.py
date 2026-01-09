import socket
import threading
import json
import hashlib
import time
import sys
from db_manager import DBManager

# --- CONFIGURAÇÃO (Em um cenário real, isso viria de um arquivo config.json) ---
# Se for rodar em maquinas reais, troque 'localhost' pelos IPs reais e use a mesma porta
NODES_CONFIG = {
    "1": {"ip": "localhost", "port": 5001, "db_host": "localhost"},
    "2": {"ip": "localhost", "port": 5002, "db_host": "localhost"},
    "3": {"ip": "localhost", "port": 5003, "db_host": "localhost"}
}
DB_USER = "root"       # <--- COLOQUE SEU USUARIO DO MYSQL
DB_PASS = "labsd"      # <--- COLOQUE SUA SENHA DO MYSQL
DB_NAME = "ddb"

class NodeMiddleware:
    def __init__(self, node_id):
        self.id = node_id
        self.ip = NODES_CONFIG[node_id]["ip"]
        self.port = NODES_CONFIG[node_id]["port"]
        self.peers = [nid for nid in NODES_CONFIG if nid != node_id] # IDs dos outros nós
        
        # Banco de dados
        self.db = DBManager(NODES_CONFIG[node_id]["db_host"], DB_USER, DB_PASS, DB_NAME)
        
        # Estado do Nó
        self.coordinator_id = max(NODES_CONFIG.keys()) # Inicialmente o maior ID é o líder (Bully simplificado)
        self.active_nodes = {}
        
    def calcular_checksum(self, payload):
        """ Gera MD5 do payload para garantir integridade """
        dump = json.dumps(payload, sort_keys=True).encode()
        return hashlib.md5(dump).hexdigest()

    def enviar_mensagem(self, target_node_id, tipo, payload):
        target = NODES_CONFIG[target_node_id]
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(2) # Timeout curto para não travar
            client.connect((target['ip'], target['port']))
            
            msg = {
                "tipo": tipo,
                "origem": self.id,
                "payload": payload,
                "checksum": self.calcular_checksum(payload)
            }
            client.send(json.dumps(msg).encode())
            print(f" -> Enviado [{tipo}] para Nó {target_node_id}")
            
            # Se for uma query, esperamos resposta imediata
            if tipo == "QUERY_REQ":
                resp = client.recv(4096).decode()
                client.close()
                return json.loads(resp)
                
            client.close()
            return True
        
        except Exception as e:
            print(f" [ERRO] Falha ao conectar com Nó {target_node_id}: {e}")
            return None

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.port))
        server.listen(5)
        print(f"[*] Middleware rodando no Nó {self.id} ({self.ip}:{self.port})")
        
        while True:
            client_sock, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client_sock,)).start()

    def handle_client(self, client_sock):
        try:
            data = client_sock.recv(4096).decode()
            if not data: return
            msg = json.loads(data)
            
            # 1. Verificar Checksum (Integridade)
            if msg['checksum'] != self.calcular_checksum(msg['payload']):
                print(" [ALERTA] Checksum inválido recebido! Pacote descartado.")
                return

            tipo = msg['tipo']
            payload = msg['payload']
            origem = msg['origem']
            # PROTOCOLO DE COMUNICACAO
            # --- PROCESSAMENTO DAS MENSAGENS ---
            
            if tipo == "HEARTBEAT":
                self.active_nodes[origem] = time.time()
                # print(f" [HEARTBEAT] Recebido de {origem}")

            elif tipo == "QUERY_REQ":
                print(f" [QUERY] Recebida de {origem}: {payload['sql']}")
                # Lógica de Distribuição
                sql = payload['sql']
                
                # Se for SELECT, executa local
                if sql.strip().upper().startswith("SELECT"):
                    res = self.db.executar_query(sql)
                    resp = {"node_exec": self.id, "result": res}
                    client_sock.send(json.dumps(resp).encode())
                
                # Se for Escrita (INSERT/UPDATE)
                else:
                    # Se EU SOU o coordenador
                    if self.id == self.coordinator_id:
                        print(" [MASTER] Sou o Coordenador. Replicando...")
                        # 1. Executa Local
                        res = self.db.executar_query(sql)
                        # 2. Replica para os outros (Broadcast)
                        for peer in self.peers:
                            self.enviar_mensagem(peer, "REPLICATE", {"sql": sql})
                        
                        resp = {"node_exec": f"{self.id} (Coordenador)", "result": res}
                        client_sock.send(json.dumps(resp).encode())
                    
                    # Se NÃO SOU coordenador, repasso para ele
                    else:
                        print(f" [SLAVE] Repassando escrita para o Coordenador {self.coordinator_id}")
                        resp = self.enviar_mensagem(self.coordinator_id, "QUERY_REQ", payload)
                        client_sock.send(json.dumps(resp).encode())

            elif tipo == "REPLICATE":
                print(f" [REPLICA] Recebendo ordem de replicação: {payload['sql']}")
                self.db.executar_query(payload['sql'])

            elif tipo == "ELECTION":
                # Algoritmo Bully Simplificado: Se recebi eleição de alguém menor, digo que estou vivo
                if int(origem) < int(self.id):
                    self.enviar_mensagem(origem, "ALIVE", {})
                    self.start_election()
            
            elif tipo == "COORDINATOR":
                self.coordinator_id = origem
                print(f" [ELEIÇÃO] Novo coordenador definido: Nó {origem}")

        except Exception as e:
            print(f"Erro no handle: {e}")
        finally:
            client_sock.close()

    def monitor_cluster(self):
        """ Thread que verifica periodicamente se o Coordenador está vivo """
        print(f" [*] Monitoramento iniciado. Coordenador atual: {self.coordinator_id}")
        
        while True:
            time.sleep(5) # Verifica a cada 5 segundos

            # Se EU SOU o coordenador, não preciso me verificar.
            # Posso apenas avisar os outros que estou vivo (opcional), 
            # mas o foco aqui é a eleição.
            if self.id == self.coordinator_id:
                continue

            # Tenta falar com o Coordenador
            print(f" [MONITOR] Pingando coordenador {self.coordinator_id}...")
            resposta = self.enviar_mensagem(self.coordinator_id, "HEARTBEAT", {})
            
            # Se a resposta for None, significa que deu erro na conexão (timeout ou recusada)
            if resposta is None:
                print(f" [ALERTA] O Coordenador {self.coordinator_id} falhou! Iniciando seleção de novo coordenador...")
                self.start_election()

    def start_election(self):
        # 1. Lista de candidatos (Eu + Peers que responderem)
        candidatos_vivos = [int(self.id)]
        
        print(" [ELEIÇÃO] Verificando quais nós estão vivos...")
        
        for peer_id in self.peers:
            # Tenta um contato rápido
            if self.enviar_mensagem(peer_id, "HEARTBEAT", {}) is not None:
                candidatos_vivos.append(int(peer_id))
        
        # 2. Quem é o maior?
        novo_lider_id = str(max(candidatos_vivos))
        
        print(f" [ELEIÇÃO] Nós vivos: {candidatos_vivos}. Vencedor: {novo_lider_id}")
        
        # 3. Atualiza o estado
        if novo_lider_id == self.id:
            self.coordinator_id = self.id
            print(f" [MASTER] EU SOU O NOVO COORDENADOR! (Nó {self.id})")
            
            # Avisa a todos que agora eu mando
            for peer in self.peers:
                self.enviar_mensagem(peer, "COORDINATOR", {})
        else:
            self.coordinator_id = novo_lider_id
            print(f" [SLAVE] Reconheço o novo coordenador: Nó {novo_lider_id}")

    def run(self):
        t_server = threading.Thread(target=self.start_server)
        t_server.start()
        
        t_monitor = threading.Thread(target=self.monitor_cluster) 
        t_monitor.start()
        
        print("Middleware Iniciado. Aguardando comandos...")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python middleware.py <NODE_ID> (1, 2 ou 3)")
        sys.exit()
    
    node_id = sys.argv[1]
    node = NodeMiddleware(node_id)
    node.run()
