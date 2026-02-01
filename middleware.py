import socket
import threading
import json
import hashlib
import time
import sys
from xmlrpc import server
from db_manager import DBManager

# Configuração dos hosts
NODES_CONFIG = {
    "1": {"ip": "localhost", "porta": 5001, "db_host": "localhost"},
    "2": {"ip": "localhost", "porta": 5002, "db_host": "localhost"},
    "3": {"ip": "localhost", "porta": 5003, "db_host": "localhost"},
}

DB_USER = "labsd"
DB_PASS = "labsd"
DB_NAME = "ddb"

class NodeMiddleware:
    def __init__(self, node_id):
        self.id = str(node_id)
        if self.id not in NODES_CONFIG:
            print(f"[ERRO] ID {self.id} não encontrado na configuração.")
            sys.exit(1)
        self.config = NODES_CONFIG[self.id]
        self.peers = [nid for nid in NODES_CONFIG if nid != self.id]

        #Banco de dados
        print(f"[INFO] Conectando ao MySQL local em {self.config['db_host']}")
        self.db = DBManager(self.config['db_host'], DB_USER, DB_PASS, DB_NAME)

        #Estado do nó
        self.coordenador_id = self.id  # Inicialmente assume que é o coordenador
        self.running = True

    # ------- Protocolo de comunicação entre nós -------

    def criar_mensagem(self, tipo, payload=None):
        #Cria o dicionario padrão para envio via JSON
        if payload is None:
            payload = {}
        
        payload_str = json.dumps(payload, sort_keys=True)
        checksum = hashlib.md5(payload_str.encode("utf-8")).hexdigest()\
        
        return {
            "tipo": tipo, 
            "origem": self.id,
            "payload": payload,
            "checksum": checksum
        }

    def validar_checksum(self, msg_dict):
        payload = msg_dict.get("payload", {})
        checksum_recebido = msg_dict.get("checksum", "")

        payload_str = json.dumps(payload, sort_keys=True)
        checksum_calculado = hashlib.md5(payload_str.encode("utf-8")).hexdigest()
        
        return checksum_recebido == checksum_calculado
    
    #--------- Comunicaçõ de Rede -----------

    def enviar_mensagem(self, target_id, tipo, payload=None, esperar_resposta=False):
        if target_id not in NODES_CONFIG:
            return None
        
        target = NODES_CONFIG[target_id]
        msg = self.criar_mensagem(tipo, payload)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((target['ip'], target['porta']))

            msg_bytes = json.dumps(msg).encode("utf-8")
            sock.send(msg_bytes)

            resposta = None
            if esperar_resposta:
                resp_bytes = sock.recv(16384)
                if resp_bytes:
                    resposta = json.loads(resp_bytes.decode("utf-8"))
            
            sock.close()
            return resposta
        
        except Exception as e:
            print(f"[INFO] Falha ao enviar para {target_id}: {e}")
            return None

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((self.config['ip'], self.config['porta']))            
            server.listen(5)
            print(f"[INFO] Servidor Middleware rodando em {self.config['ip']}:{self.config['porta']}")
            
            while self.running:
                client_socket, addr = server.accept()
                threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()

        except Exception as e:
            print(f" [ERRO] Erro ao iniciar servidor: {e}")
            sys.exit(1)

    def handle_client(self, cliente_socket):
        try:
            data = cliente_socket.recv(16384).decode("utf-8") 
            if not data: return

            msg = json.loads(data)

            if not self.validar_checksum(msg):
                print(f"[ERRO] Checksum inválido de {msg.get('origem')}")
                return
            
            response = self.processar_mensagem(msg)

            if response:
                cliente_socket.sendall(json.dumps(response).encode("utf-8"))
        
        except json.JSONDecodeError:
            print(" [ERRO] Recebido JSON malformado.")
        except Exception as err:
            print(f"[ERRO] Processando cliente: {err}")
        finally:
            cliente_socket.close()
            
    def aplicar_dump(self, dump_dados):
        print("[SYNC] Iniciando reconstrução do banco de dados...")
        
        # 1. Desliga verificação de chaves estrangeiras para poder apagar/criar sem erro de ordem
        self.db.executar_query("SET FOREIGN_KEY_CHECKS = 0")
        
        # 2. Apaga todas as tabelas existentes (Limpeza)
        res = self.db.executar_query("SHOW TABLES")
        if res.get("status") == "OK":
            tabelas_atuais = [list(r.values())[0] for r in res["dados"]]
            for t in tabelas_atuais:
                self.db.executar_query(f"DROP TABLE IF EXISTS {t}")

        # 3. Recria tabelas e insere dados
        for table_name, data in dump_dados.items():
            print(f"[SYNC] Restaurando tabela: {table_name}")
            
            # Cria a tabela
            create_sql = data.get("schema")
            if create_sql:
                self.db.executar_query(create_sql)
            
            # Insere os dados
            rows = data.get("rows", [])
            if rows:
                for row in rows:
                    # Monta o INSERT manualmente (Cuidado: SQL Injection simples aqui, ok para lab)
                    colunas = list(row.keys())
                    valores = []
                    for v in row.values():
                        if v is None:
                            valores.append("NULL")
                        else:
                            # Escapa aspas simples para não quebrar o SQL
                            val_str = str(v).replace("'", "''").replace("\\", "\\\\")
                            valores.append(f"'{val_str}'")
                    
                    sql = f"INSERT INTO {table_name} ({', '.join(colunas)}) VALUES ({', '.join(valores)})"
                    self.db.executar_query(sql)
        
        # 4. Religa as chaves estrangeiras
        self.db.executar_query("SET FOREIGN_KEY_CHECKS = 1")
        print("[SYNC] Banco de dados sincronizado com sucesso!")
    
    def processar_mensagem(self, msg):
        tipo = msg["tipo"]
        origem = msg["origem"]
        payload = msg["payload"]

        # 1. Requisições do Cliente (Query)
        if tipo == "QUERY_REQ":
            sql = payload.get("sql", "")
            print(f"[QUERY] Recebida de {origem}: {sql}")

            read_keywords = ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]
            is_read = any(sql.strip().upper().startswith(k) for k in read_keywords)

            if is_read:
                # Leitura pode ser feita em qualquer nó (eventual consistency)
                resultado = self.db.executar_query(sql)
                return self.criar_mensagem("QUERY_RESP", resultado)
            else:
                # Escrita (INSERT/UPDATE) só no Coordenador
                if self.id == self.coordenador_id:
                    print(f"[ESCRITA] Sou coordenador. Executando e replicando.")
                    res = self.db.executar_query(sql)

                    # Replicar para os outros nós
                    if res["status"] == "OK":
                        self.replicar_dados(sql)
                    
                    return self.criar_mensagem("QUERY_RESP", res)
                else:
                    print(f"[ESCRITA] Redirecionando para coordenador {self.coordenador_id}")
                    # Repassa a requisição para o coordenador
                    resp = self.enviar_mensagem(self.coordenador_id, "QUERY_REQ", payload, esperar_resposta=True)
                    if resp:
                        return resp
                    else:
                        return self.criar_mensagem("ERRO", {"mensagem": "Coordenador indisponível"})
        
        # 2. Replicação
        elif tipo == "REPLICACAO":
            sql = payload.get("sql")
            print(f"[REPLICA] Aplicando: {sql}")
            self.db.executar_query(sql)
            return self.criar_mensagem("ACK")   

        # 3. Eleição e Heartbeat
        elif tipo == "HEARTBEAT":
            return self.criar_mensagem("VIVO")

        elif tipo == "ELEICAO":
            print(f"[ELEIÇÃO] Recebida de {origem}")     
            if int(self.id) > int(origem):
                threading.Thread(target=self.inicar_eleicao).start()
                return self.criar_mensagem("VIVO")
            
        elif tipo == "COORDENADOR":
            self.coordenador_id = origem
            print(f"[ELEICAO] Novo coordenador reconhecido: {origem}")

        # 4. Sincronização (Novo nó entrando)
        elif tipo == "QUEM_E_O_CHEFE":
            if self.id == self.coordenador_id:
                return self.criar_mensagem("EU_SOU_O_CHEFE")
        
        elif tipo == "SYNC_REQ":
            print(f"[SYNC] Recebida solicitação de {origem}. Gerando dump completo...")
            # Pega o banco inteiro agora
            dump_completo = self.db.get_full_dump()
            return self.criar_mensagem("SYNC_DATA", dump_completo)
        
        return None

    def replicar_dados(self, sql):
        for peer in self.peers:
            threading.Thread(target=self.enviar_mensagem, args=(peer, "REPLICACAO", {"sql": sql})).start()
            
    def iniciar_eleicao(self):
        print(f" [ELEIÇÃO] Iniciando processo de eleição")
        sou_o_maior = True

    # Pergunta para todos com ID maior se estão vivos        
        for peer in self.peers:
            if int(peer) > int(self.id):
                resp = self.enviar_mensagem(peer, "ELEICAO", esperar_resposta=True)
                if resp and resp["tipo"] == "ALIVE":
                    sou_o_maior = False
                    break
        
        if sou_o_maior:
            self.tornar_coordenador()
    
    def tornar_coordenador(self):
        self.coordenador_id = self.id
        print(f" [ELEIÇÃO] Eu sou o novo coordenador (Nó {self.id})")
        for peer in self.peers:
            self.enviar_mensagem(peer, "COORDENADOR")
    
    def monitorar_coordenador(self):
        print(" [MONITOR] Iniciando.")
        while self.running:
            time.sleep(5)
            if self.id == self.coordenador_id:
                continue  # Coordenador não monitora a si mesmo

            resp = self.enviar_mensagem(self.coordenador_id, "HEARTBEAT", esperar_resposta=True)
            if not resp:
                print(f"[ALERTA] Coordenador {self.coordenador_id} sumiu. Convocando eleição.")
                self.iniciar_eleicao()
    

    def join_cluster(self):
        print("[JOIN] Entrando no cluster.")    
        coordenador_encontrado = None
        print("[JOIN] Buscando coordenador na rede")
        for peer in self.peers:
            resp = self.enviar_mensagem(peer, "QUEM_E_O_CHEFE", esperar_resposta=True)
            if resp and resp["tipo"] == "EU_SOU_O_CHEFE":
                coordenador_encontrado = peer
                break
                
        if coordenador_encontrado:
            self.coordenador_id = coordenador_encontrado
            print(f"[JOIN] Coordenador é {self.coordenador_id}. Pedindo sincronização.")
            # Pede dados
            resp = self.enviar_mensagem(self.coordenador_id, "SYNC_REQ", esperar_resposta=True)

            if resp and resp["tipo"] == "SYNC_DATA":
                dump_recebido = resp.get("payload", {})
                if dump_recebido:
                    self.aplicar_dump(dump_recebido)
                else:
                    print("[JOIN] Recebido dump vazio ou inválido.")
        else:
            print("[JOIN] Ninguém respondeu. Assumindo liderança.")
            self.coordenador_id = self.id
  
    def run(self):
        # 1. Inicia o servidor 
        threading.Thread(target=self.start_server, daemon=True).start()
        time.sleep(1) # Espera servidor subir

        # 2. Entra no Cluter
        self.join_cluster()

        # 3. Monitora
        threading.Thread(target=self.monitorar_coordenador, daemon=True).start()
        
        print(f"[INFO] Nó {self.id} rodando (Coord: {self.coordenador_id}).")

        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            print("Saindo.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python middleware.py <ID_NO>")
    else:
        NodeMiddleware(sys.argv[1]).run()
