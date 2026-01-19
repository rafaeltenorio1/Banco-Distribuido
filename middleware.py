import socket
import threading
import json
import hashlib
import time
import sys
from db_manager import DBManager

# Configuração dos hosts
NODES_CONFIG = {
    "1": {"ip": "172.20.10.12", "porta": 5001, "db_host": "localhost"},
    "2": {"ip": "172.20.10.9", "porta": 5001, "db_host": "localhost"}
    "3": {"ip": "192.0.0.2", "porta": 5001, "db_host": "localhost"}
    }

DB_USER = "labsd"
DB_PASS = "labsd"
DB_NAME = "ddb"

class NodeMiddleware:
    def __init__(self, node_id):
        self.id = node_id
        self.ip = NODES_CONFIG[node_id]["ip"]
        self.porta = NODES_CONFIG[node_id]["porta"]
        # Lista de IDs dos outros nós
        self.pares = [nid for nid in NODES_CONFIG if nid != node_id] 
        
        # Conexão com o Banco de Dados Local
        print(f" [INFO] Conectando ao MySQL local ({NODES_CONFIG[node_id]['db_host']}).")
        self.db = DBManager(NODES_CONFIG[node_id]["db_host"], DB_USER, DB_PASS, DB_NAME)
        
        # Estado do Nó
        self.coordenador_id = self.id # Inicia como lider
        self.active_nodes = {}
        
    def calcular_checksum(self, payload):
        # Função para verificar a carga com checksum
        dump = json.dumps(payload, sort_keys=True).encode()
        return hashlib.md5(dump).hexdigest()

    def enviar_mensagem(self, target_node_id, tipo, payload):
        # Função para enviar uma mensagem para outro nó
        target = NODES_CONFIG[target_node_id]
        try:
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cliente.settimeout(2) # Timeout curto para não travar o sistema
            cliente.connect((target['ip'], target['porta']))
            
            mensagem = {
                "tipo": tipo,
                "origem": self.id,
                "payload": payload,
                "checksum": self.calcular_checksum(payload)
            }
            cliente.send(json.dumps(mensagem).encode())
            
            # Caso seja uma query o cliente espera a resposta
            if tipo == "QUERY_REQ":
                resp_raw = cliente.recv(409600).decode()
                cliente.close()
                return json.loads(resp_raw)
                
            cliente.close()
            return True 
        
        except Exception as e:
            # Caso o nó esteja offline, apenas retorna None
            return None

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.bind((self.ip, self.porta))
            server.listen(5)
            print(f"[INFO] Servidor Middleware rodando em {self.ip}:{self.porta}")
            
            while True:
                cliente_socket, _ = server.accept()
                # Cria uma thread para cada requisição recebida
                t = threading.Thread(target=self.comunica_cliente, args=(cliente_socket,))
                t.daemon = True
                t.start()
        except Exception as err:
            print(f" [ERRO] Erro ao iniciar servidor: {err}")
            sys.exit(1)

    def comunica_cliente(self, cliente_socket):
        try:
            
            data = cliente_socket.recv(409600).decode() 
            if not data: 
                return 
            mensagem = json.loads(data)
            
            # Valida a integridade da mensagem através do checksum, caso esteja incorreto, descarta a mensagem
            if mensagem['checksum'] != self.calcular_checksum(mensagem['payload']):
                print(" [ERRO] Checksum inválido. Mensagem descartada")
                return

            tipo = mensagem['tipo']
            payload = mensagem['payload']
            origem = mensagem['origem']

            # Inicio do protocolo de comunicação

            
            if tipo == "HEARTBEAT":
                self.active_nodes[origem] = time.time()

            elif tipo == "QUERY_REQ":
                print(f" [INFO] Query recebida de {origem}: {payload['sql']}")
                sql = payload['sql']
                
                # Caso a query seja de SELECT
                if sql.strip().upper().startswith("SELECT"):
                    resultado = self.db.executar_query(sql)
                    resposta = {"node_exec": self.id, "resultado": resultado}
                    cliente_socket.send(json.dumps(resposta).encode())
                
                # Caso a query seja de INSERT/UPDATE/DELETE
                else:
                    # Caso seja o coordenador executa a query e replica para os demais
                    if self.id == self.coordenador_id:
                        print(" [INFO] Executando query e replicando")
                        resultado = self.db.executar_query(sql)
                        
                        # Realizando o broadcast para replicação
                        for par in self.pares:
                            threading.Thread(target=self.enviar_mensagem, 
                                           args=(par, "REPLICACAO", {"sql": sql})).start()
                        
                        resposta = {"node_exec": f"{self.id}", "resultado": resultado}
                        cliente_socket.send(json.dumps(resposta).encode())
                    
                    # Caso não seja o coordenador, redireciona a query para o coordenador
                    else:
                        print(f" [INFO] Redirecionando a query para o coordenador {self.coordenador_id}")
                        resposta = self.enviar_mensagem(self.coordenador_id, "QUERY_REQ", payload)
                        cliente_socket.send(json.dumps(resposta).encode())

            elif tipo == "REPLICACAO":
                print(f" [REPLICACAO] Gravando no banco local: {payload['sql']}")
                self.db.executar_query(payload['sql'])
            
            # Realização da sincronização e identificacão do coordenador
            elif tipo == "PROCURAR_COORDENADOR":
                # Caso seja o coordenador, responde o nó solicitante.
                if self.id == self.coordenador_id:
                    print(f" [INFO] Nó {origem} procurando o coordenandor. Respondendo")
                    # Responde direto pra quem perguntou
                    self.enviar_mensagem(origem, "ANUNCIA_COORDENADOR", {})

            elif tipo == "ANUNCIA_COORDENADOR":
                self.coordenador_id = origem
                print(f" [INFO] Coordenador confirmado: {origem}")

            elif tipo == "SYNC_REQ":
                print(f" [SYNC] Nó {origem} solicitou sincronização.")
                dados_banco = self.db.executar_query("SELECT * FROM clientes")
                lista = dados_banco.get('dados', [])
                
                # Envia a resposta com os dados para a sincronização
                self.enviar_mensagem(origem, "SYNC_DATA", {"clientes": lista})
                print(f" [SYNC] {len(lista)} registros enviados para {origem}.")

            elif tipo == "SYNC_DATA":
                print(" [SYNC] Recebendo dados.")
                lista_clientes = payload['clientes']
                self.atualizar_banco_local(lista_clientes)
            
            elif tipo == "ELEICAO":
                # O ID maior responde VIVO e inicia a eleição para se tornar coordenador
                if int(self.id) > int(origem):
                    self.enviar_mensagem(origem, "VIVO", {})
                    self.iniciar_eleicao()
            
            elif tipo == "COORDENADOR":
                self.coordenador_id = origem
                print(f" [INFO] Novo coordenador escolhido: Nó {origem}")

        except Exception as err:
            print(f" [ERRO] Handler: {err}")
        finally:
            cliente_socket.close()

    def atualizar_banco_local(self, lista_clientes):
        try:
            self.db.executar_query("DELETE FROM clientes") 
            for cliente in lista_clientes:
                nome = cliente['nome']
                email = cliente['email']
                sql = f"INSERT INTO clientes (nome, email) VALUES ('{nome}', '{email}')"
                self.db.executar_query(sql)
                
            
            print(f" [SYNC] Sincronização Finalizada.")
        except Exception as err:
            print(f" [ERRO] Falha na sincronização: {err}")

    # Inicialização de um nó
    def join_cluster(self):
        print(f" [INFO] Iniciando join no sistema (Nó {self.id})")
        
        # Identifica o coordenador atual
        print(" [JOIN] Buscando coordenador na rede")
        for par in self.pares:
            self.enviar_mensagem(par, "PROCURAR_COORDENADOR", {})
        
        time.sleep(3)
        
        # Caso não encontra nenhum coordenador, assume a liderança
        if self.coordenador_id == self.id:
            print(" [JOIN] Nenhuma resposta do coordenador. Assumindo liderança.")
        # Caso encontre um coordenador, inicia a sincronização dos dados
        else:
            print(f" [JOIN] Coordenador encontrado, Nó {self.coordenador_id}.")
            print(" [JOIN] Solicitando sincronização de dados")
    
            self.enviar_mensagem(self.coordenador_id, "SYNC_REQ", {})
            
            print(" [JOIN] Aguardando transferência de dados")
            time.sleep(3) 

            # Caso entre e o ID do novo Nó seja maior que o do coordenador, inicia a eleição para virar o coordenador
            if int(self.id) > int(self.coordenador_id):
                print(f" [INFO] ID do nó {self.id} é maior que o do coordenador atual (Nó {self.coordenador_id}).")
                print(" [INFO] Iniciando eleição para se tornar coordenador.")
                self.iniciar_eleicao()

    def monitor_cluster(self):
        # Verifica periodicamente se o Coordenador está vivo
        print(" [INFO] Monitor de cluster ativo.")
        while True:
            time.sleep(5) 
            
            # Caso seja o coordenador, não precisa monitorar
            if self.id == self.coordenador_id:
                continue
                
            # Tenta pingar o coordenador
            print(f" [MONITORANDO] Verificando coordenador {self.coordenador_id}")
            resultado = self.enviar_mensagem(self.coordenador_id, "HEARTBEAT", {})
            
            if resultado is None:
                print(f" [ALERTA] Coordenador {self.coordenador_id} não responde. Iniciando eleição.")
                self.iniciar_eleicao()

    def iniciar_eleicao(self):
        print(f" [ELEIÇÃO] Iniciando eleição")
        
        # Envia ELEICAO para todos os nós vivos
        candidatos_vivos = [int(self.id)]
        
        for par in self.pares:
            if self.enviar_mensagem(par, "HEARTBEAT", {}) is not None:
                candidatos_vivos.append(int(par))
        
        # Identifica o maior ID entre os vivos para virar o novo coordenador
        novo_lider = str(max(candidatos_vivos))
        
        # Caso seja o coordenador, anuncia para os outros nós
        if novo_lider == self.id:
            self.coordenador_id = self.id
            print(f" [INFO] Nó {self.id} é o novo coordenador.")
            for par in self.pares:
                self.enviar_mensagem(par, "COORDENADOR", {})
        # Caso não seja o coordenador, reconhece o novo
        else:
            self.coordenador_id = novo_lider
            print(f" [ELEIÇÃO] Reconheço o nó {novo_lider} como novo coordenador.")

  
    def run(self):
        #Inicia o servidor 
        t_server = threading.Thread(target=self.start_server)
        t_server.daemon = True
        t_server.start()
        time.sleep(1)

        # Executa o join no cluster
        self.join_cluster()

        # 3. Inicia o monitoramento
        t_monitor = threading.Thread(target=self.monitor_cluster)
        t_monitor.daemon = True
        t_monitor.start()
        
        print(f" [INFO] Nó {self.id} rodando.")
        
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            print("\nEncerrando middleware")
            sys.exit(0)

if __name__ == "__main__":

    node_id = sys.argv[1]
    if node_id not in NODES_CONFIG:
        print(f"[ERRO] ID {node_id} não configurado.")
        sys.exit()

    app = NodeMiddleware(node_id)
    app.run()
