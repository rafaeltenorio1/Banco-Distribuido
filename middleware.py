import socket
import threading
import json
import hashlib
import time
import sys
from db_manager import DBManager

# --- CONFIGURAÇÃO ---
# ATENÇÃO: Em produção, substitua 'localhost' pelos IPs reais das máquinas.
NODES_CONFIG = {
    "1": {"ip": "192.168.15.48", "port": 5001, "db_host": "localhost"},
    "2": {"ip": "192.168.15.6", "port": 5001, "db_host": "localhost"}
    # Adicione mais nós se necessário...
}

DB_USER = "labsd"       # <--- SEU USUARIO
DB_PASS = "labsd"       # <--- SUA SENHA
DB_NAME = "ddb"

class NodeMiddleware:
    def __init__(self, node_id):
        self.id = node_id
        self.ip = NODES_CONFIG[node_id]["ip"]
        self.port = NODES_CONFIG[node_id]["port"]
        # Lista de IDs dos outros nós (Peers)
        self.peers = [nid for nid in NODES_CONFIG if nid != node_id] 
        
        # Conexão com o Banco de Dados Local
        print(f" [*] Conectando ao MySQL local ({NODES_CONFIG[node_id]['db_host']})...")
        self.db = DBManager(NODES_CONFIG[node_id]["db_host"], DB_USER, DB_PASS, DB_NAME)
        
        # Estado do Nó
        self.coordinator_id = self.id # Começa achando que é o líder até provarem o contrário
        self.active_nodes = {}
        
    def calcular_checksum(self, payload):
        """ Gera MD5 do payload para garantir integridade """
        dump = json.dumps(payload, sort_keys=True).encode()
        return hashlib.md5(dump).hexdigest()

    def enviar_mensagem(self, target_node_id, tipo, payload):
        """ Função genérica para enviar JSON via Socket """
        target = NODES_CONFIG[target_node_id]
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(2) # Timeout curto para não travar o sistema
            client.connect((target['ip'], target['port']))
            
            msg = {
                "tipo": tipo,
                "origem": self.id,
                "payload": payload,
                "checksum": self.calcular_checksum(payload)
            }
            client.send(json.dumps(msg).encode())
            
            # Se for uma QUERY, o cliente espera resposta imediata (síncrono)
            if tipo == "QUERY_REQ":
                resp_raw = client.recv(409600).decode() # Buffer grande para respostas
                client.close()
                return json.loads(resp_raw)
                
            client.close()
            return True # Sucesso
        
        except Exception as e:
            # Em caso de erro (nó offline), apenas retorna None
            return None

    # -------------------------------------------------------------------------
    # SERVIDOR (Ouvido do Middleware)
    # -------------------------------------------------------------------------
    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.bind((self.ip, self.port))
            server.listen(5)
            print(f"[*] Servidor Middleware rodando em {self.ip}:{self.port}")
            
            while True:
                client_sock, addr = server.accept()
                # Cria uma thread para cada requisição recebida
                t = threading.Thread(target=self.handle_client, args=(client_sock,))
                t.daemon = True
                t.start()
        except Exception as e:
            print(f" [FATAL] Erro ao iniciar servidor: {e}")
            sys.exit(1)

    def handle_client(self, client_sock):
        try:
            # Buffer aumentado para receber grandes cargas de dados (SYNC)
            data = client_sock.recv(409600).decode() 
            if not data: return
            msg = json.loads(data)
            
            # Validação de Integridade
            if msg['checksum'] != self.calcular_checksum(msg['payload']):
                print(" [ALERTA] Checksum inválido. Pacote ignorado.")
                return

            tipo = msg['tipo']
            payload = msg['payload']
            origem = msg['origem']

            # --- ROTEAMENTO DE MENSAGENS ---

            if tipo == "HEARTBEAT":
                self.active_nodes[origem] = time.time()

            elif tipo == "QUERY_REQ":
                print(f" [QUERY] Recebida de {origem}: {payload['sql']}")
                sql = payload['sql']
                
                # LEITURA (SELECT) -> Executa Localmente
                if sql.strip().upper().startswith("SELECT"):
                    res = self.db.executar_query(sql)
                    resp = {"node_exec": self.id, "result": res}
                    client_sock.send(json.dumps(resp).encode())
                
                # ESCRITA (INSERT/UPDATE)
                else:
                    # Se SOU o Coordenador: Executo e Replico
                    if self.id == self.coordinator_id:
                        print(" [MASTER] Executando escrita e replicando...")
                        res = self.db.executar_query(sql)
                        
                        # Broadcast para escravos
                        for peer in self.peers:
                            threading.Thread(target=self.enviar_mensagem, 
                                           args=(peer, "REPLICATE", {"sql": sql})).start()
                        
                        resp = {"node_exec": f"{self.id} (MASTER)", "result": res}
                        client_sock.send(json.dumps(resp).encode())
                    
                    # Se NÃO sou Coordenador: Repasso para ele
                    else:
                        print(f" [SLAVE] Redirecionando escrita para Mestre {self.coordinator_id}...")
                        resp = self.enviar_mensagem(self.coordinator_id, "QUERY_REQ", payload)
                        client_sock.send(json.dumps(resp).encode())

            elif tipo == "REPLICATE":
                print(f" [REPLICA] Gravando no banco local: {payload['sql']}")
                self.db.executar_query(payload['sql'])

            # --- PROTOCOLO DE DESCOBERTA E SYNC ---
            
            elif tipo == "WHO_IS_MASTER":
                # Se eu sou o mestre, eu respondo.
                if self.id == self.coordinator_id:
                    print(f" [MASTER] Nó {origem} perguntou quem manda. Respondendo...")
                    # Respondo direto pra quem perguntou (não broadcast)
                    self.enviar_mensagem(origem, "COORDINATOR_ANNOUNCE", {})

            elif tipo == "COORDINATOR_ANNOUNCE":
                self.coordinator_id = origem
                # print(f" [INFO] Coordenador confirmado: {origem}")

            elif tipo == "SYNC_REQ":
                print(f" [SYNC] Nó {origem} solicitou sincronização total.")
                # Pega todos os dados do banco
                dados_banco = self.db.executar_query("SELECT * FROM clientes")
                lista = dados_banco.get('dados', [])
                
                # Envia de volta
                self.enviar_mensagem(origem, "SYNC_DATA", {"clientes": lista})
                print(f" [SYNC] {len(lista)} registros enviados para {origem}.")

            elif tipo == "SYNC_DATA":
                print(" [SYNC] Recebendo carga de dados...")
                lista_clientes = payload['clientes']
                self.atualizar_banco_local(lista_clientes)

            # --- ELEIÇÃO (ALGORITMO BULLY) ---
            
            elif tipo == "ELECTION":
                # Se meu ID é maior, respondo ALIVE e inicio minha eleição
                if int(self.id) > int(origem):
                    self.enviar_mensagem(origem, "ALIVE", {})
                    self.start_election()
            
            elif tipo == "COORDINATOR":
                self.coordinator_id = origem
                print(f" [ELEIÇÃO] Novo Líder Aclamado: Nó {origem}")

        except Exception as e:
            print(f" [ERRO] Handler: {e}")
        finally:
            client_sock.close()

    def atualizar_banco_local(self, lista_clientes):
        """ Apaga o banco local e reescreve com os dados recebidos """
        try:
            # 1. Limpa tabela
            self.db.executar_query("DELETE FROM clientes") # Use TRUNCATE se preferir zerar IDs
            
            # 2. Reinsere dados
            count = 0
            for cliente in lista_clientes:
                # Adapte as chaves conforme o retorno do seu DBManager (dicionário ou tupla)
                # Assumindo dicionário: {'id': 1, 'nome': 'Fulano', ...}
                nome = cliente['nome']
                email = cliente['email']
                sql = f"INSERT INTO clientes (nome, email) VALUES ('{nome}', '{email}')"
                self.db.executar_query(sql)
                count += 1
            
            print(f" [SYNC] Sincronização Finalizada. {count} registros importados com sucesso.")
        except Exception as e:
            print(f" [ERRO] Falha crítica na sincronização: {e}")

    # -------------------------------------------------------------------------
    # LÓGICA DE INICIALIZAÇÃO (JOIN)
    # -------------------------------------------------------------------------
    def join_cluster(self):
        print("\n" + "="*40)
        print(f" [*] INICIANDO PROTOCOLO DE JOIN (NÓ {self.id})")
        print("="*40)
        
        # 1. Perguntar na rede: "Tem algum mestre aí?"
        print(" [JOIN] Buscando Coordenador na rede...")
        for peer in self.peers:
            self.enviar_mensagem(peer, "WHO_IS_MASTER", {})
        
        # 2. Aguarda respostas (handle_client vai atualizar self.coordinator_id se receber resposta)
        time.sleep(3)
        
        # 3. Análise
        if self.coordinator_id == self.id:
            print(" [JOIN] Nenhuma resposta de Mestre. Assumindo Liderança Inicial.")
        else:
            print(f" [JOIN] Encontrado Mestre no Nó {self.coordinator_id}.")
            print(" [JOIN] Solicitando Sincronização de Dados...")
            
            # 4. Pede os dados
            self.enviar_mensagem(self.coordinator_id, "SYNC_REQ", {})
            
            # 5. Aguarda os dados chegarem e serem processados
            print(" [JOIN] Aguardando transferência de dados...")
            time.sleep(3) 
            
            # 6. Check do Algoritmo Bully Pós-Sync
            # Se eu entrei, peguei os dados, mas meu ID é maior que o do chefe atual, eu o derrubo.
            if int(self.id) > int(self.coordinator_id):
                print(f" [BULLY] Meu ID ({self.id}) é maior que o do Mestre atual ({self.coordinator_id}).")
                print(" [BULLY] Iniciando Eleição para tomar a liderança...")
                self.start_election()
                
        print("="*40 + "\n")

    # -------------------------------------------------------------------------
    # MONITORAMENTO E ELEIÇÃO
    # -------------------------------------------------------------------------
    def monitor_cluster(self):
        """ Verifica periodicamente se o Coordenador está vivo """
        print(" [*] Monitor de Cluster Ativo.")
        while True:
            time.sleep(5) 
            
            # Se eu sou o chefe, não me monitoro
            if self.id == self.coordinator_id:
                continue
                
            # Tenta pingar o chefe
            # print(f" [MONITOR] Verificando Mestre {self.coordinator_id}...")
            res = self.enviar_mensagem(self.coordinator_id, "HEARTBEAT", {})
            
            if res is None:
                print(f" [ALERTA] Mestre {self.coordinator_id} não responde! Iniciando Eleição.")
                self.start_election()

    def start_election(self):
        print(f" [ELEIÇÃO] Convocando eleição...")
        
        # 1. Envia ELECTION para todos com ID maior que o meu (Simplificação Bully)
        # Na verdade, enviaremos para todos ver quem está vivo para simplificar o código
        candidatos_vivos = [int(self.id)]
        
        for peer in self.peers:
            if self.enviar_mensagem(peer, "HEARTBEAT", {}) is not None:
                candidatos_vivos.append(int(peer))
        
        # 2. Quem é o maior ID vivo?
        novo_lider = str(max(candidatos_vivos))
        
        # 3. Se sou eu, aviso todo mundo. Se não, aceito.
        if novo_lider == self.id:
            self.coordinator_id = self.id
            print(f" [MASTER] Venci a eleição! Sou o novo Coordenador.")
            for peer in self.peers:
                self.enviar_mensagem(peer, "COORDINATOR", {})
        else:
            self.coordinator_id = novo_lider
            print(f" [ELEIÇÃO] Reconheço {novo_lider} como novo Mestre.")

    # -------------------------------------------------------------------------
    # LOOP PRINCIPAL
    # -------------------------------------------------------------------------
    def run(self):
        # 1. Inicia o Servidor em Background
        t_server = threading.Thread(target=self.start_server)
        t_server.daemon = True
        t_server.start()
        
        # Aguarda servidor subir
        time.sleep(1)
        
        # 2. Executa Protocolo de Entrada (Busca Mestre -> Sync -> Bully)
        self.join_cluster()
        
        # 3. Inicia Monitoramento
        t_monitor = threading.Thread(target=self.monitor_cluster)
        t_monitor.daemon = True
        t_monitor.start()
        
        print(f" [*] Nó {self.id} rodando e aguardando comandos. (Ctrl+C para sair)")
        
        # Mantém script rodando
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            print("\nEncerrando Middleware...")
            sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python middleware.py <NODE_ID> (Ex: 1 ou 2)")
        sys.exit()
    
    node_id = sys.argv[1]
    if node_id not in NODES_CONFIG:
        print(f"Erro: ID {node_id} não configurado em NODES_CONFIG.")
        sys.exit()

    app = NodeMiddleware(node_id)
    app.run()
