import socket
import threading
import json
import hashlib
import time
import sys
from db_manager import DBManager

# Configuração dos hosts
NODES_CONFIG = {
    "1": {"ip": "localhost", "porta": 5001, "db_host": "localhost"},
    "2": {"ip": "localhost", "porta": 5002, "db_host": "localhost"},
    "3": {"ip": "localhost", "porta": 5003, "db_host": "localhost"},
}

DB_USER = "root"
DB_PASS = "admin"
DB_NAME = "ddb"

class NodeMiddleware:
    def __init__(self, node_id):
        self.id = str(node_id)
        if self.id not in NODES_CONFIG:
            print(f"[ERRO] ID {self.id} não encontrado na configuração.")
            sys.exit(1)
        self.config = NODES_CONFIG[self.id]
        self.peers = [nid for nid in NODES_CONFIG if nid != self.id]

        print("------------------------------------------------")
        print(f"[INIT] Iniciando Nó {self.id}")
        self.db = DBManager(self.config['db_host'], DB_USER, DB_PASS, DB_NAME)
        self.coordenador_id = self.id
        self.running = True

    # ------- Protocolo -------
    def criar_mensagem(self, tipo, payload=None):
        if payload is None: payload = {}
        payload_str = json.dumps(payload, sort_keys=True)
        checksum = hashlib.md5(payload_str.encode("utf-8")).hexdigest()
        return {"tipo": tipo, "origem": self.id, "payload": payload, "checksum": checksum}

    def validar_checksum(self, msg_dict):
        payload = msg_dict.get("payload", {})
        checksum_recebido = msg_dict.get("checksum", "")
        payload_str = json.dumps(payload, sort_keys=True)
        return checksum_recebido == hashlib.md5(payload_str.encode("utf-8")).hexdigest()
    
    # --------- Rede -----------
    def enviar_mensagem(self, target_id, tipo, payload=None, esperar_resposta=False):
        if target_id not in NODES_CONFIG: return None
        target = NODES_CONFIG[target_id]
        msg = self.criar_mensagem(tipo, payload)

        try:
            # print(f"[NET OUT] Enviando {tipo} para Nó {target_id}...") 
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5) # Timeout maior para Dumps
            sock.connect((target['ip'], target['porta']))
            sock.send(json.dumps(msg).encode("utf-8"))

            resposta = None
            if esperar_resposta:
                # Loop de leitura para mensagens grandes
                chunks = []
                while True:
                    try:
                        chunk = sock.recv(4096)
                        if not chunk: break
                        chunks.append(chunk)
                    except socket.timeout:
                        break
                
                if chunks:
                    full_data = b''.join(chunks).decode("utf-8")
                    resposta = json.loads(full_data)
                    # print(f"[NET IN] Resposta recebida de {target_id} (Tamanho: {len(full_data)} bytes)")
            
            sock.close()
            return resposta
        except Exception as e:
            print(f"[NET ERROR] Falha ao enviar para {target_id}: {e}")
            return None

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((self.config['ip'], self.config['porta']))            
            server.listen(5)
            print(f"[SERVER] Rodando em {self.config['ip']}:{self.config['porta']}")
            while self.running:
                client_socket, _ = server.accept()
                threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()
        except Exception as e:
            print(f"[SERVER FATAL] {e}")
            sys.exit(1)

    def handle_client(self, cliente_socket):
        try:
            # Leitura robusta
            chunks = []
            while True:
                cliente_socket.settimeout(2.0)
                try:
                    chunk = cliente_socket.recv(4096)
                    if not chunk: break
                    chunks.append(chunk)
                    if len(chunk) < 4096: break # Fim provável
                except socket.timeout:
                    break
            
            if not chunks: return
            data = b''.join(chunks).decode("utf-8")
            msg = json.loads(data)

            if not self.validar_checksum(msg):
                print(f"[SEC] Checksum inválido de {msg.get('origem')}")
                return
            
            response = self.processar_mensagem(msg)
            if response:
                cliente_socket.sendall(json.dumps(response).encode("utf-8"))
        except Exception as err:
            print(f"[SERVER ERROR] {err}")
        finally:
            cliente_socket.close()

    # --------- LÓGICA DE APLICAÇÃO DO DUMP -----------
    def aplicar_dump(self, dump_dados):
        print("\n[SYNC START] Iniciando Restore do Banco...")
        
        if not dump_dados:
            print("[SYNC] Dump vazio.")
            return

        try:
            self.db.executar_query("SET FOREIGN_KEY_CHECKS = 0")
            
            count_bancos = 0
            
            for key, data in dump_dados.items():
                db_name = data.get("database")
                table_name = data.get("table")
                create_sql = data.get("schema")
                rows = data.get("rows", [])
                
                # Validação
                if not db_name or str(db_name) in ["None", "null"]:
                    print(f"[SYNC SKIP] Chave inválida encontrada: {key}")
                    continue

                print(f"[SYNC STEP] Processando Banco: {db_name}")
                
                # 1. Cria Banco
                res = self.db.executar_query(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                if res['status'] == 'ERRO':
                    print(f"   [!!!] Erro ao criar banco: {res['mensagem']}")
                    continue
                
                if not table_name: 
                    # É um banco vazio, termina aqui
                    continue

                # 2. Muda Contexto (USE)
                res = self.db.executar_query(f"USE {db_name}")
                if res['status'] == 'ERRO':
                    print(f"   [!!!] Erro ao entrar no banco: {res['mensagem']}")
                    continue

                # 3. Cria Tabela
                print(f"   -> Recriando tabela '{table_name}'...")
                self.db.executar_query(f"DROP TABLE IF EXISTS {table_name}")
                if create_sql:
                    self.db.executar_query(create_sql)
                
                # 4. Insere Dados
                if rows:
                    print(f"   -> Inserindo {len(rows)} registros...")
                    for row in rows:
                        colunas = list(row.keys())
                        valores = []
                        for v in row.values():
                            if v is None: valores.append("NULL")
                            elif isinstance(v, (int, float)): valores.append(str(v))
                            else:
                                val_str = str(v).replace("'", "''").replace("\\", "\\\\")
                                valores.append(f"'{val_str}'")
                        
                        sql = f"INSERT INTO {table_name} ({', '.join(colunas)}) VALUES ({', '.join(valores)})"
                        self.db.executar_query(sql)

            self.db.executar_query("SET FOREIGN_KEY_CHECKS = 1")
            print("[SYNC END] Sincronização Finalizada!\n")

        except Exception as e:
            print(f"[SYNC FATAL] {e}")

    # --------- Processamento de Mensagens -----------
    def processar_mensagem(self, msg):
        tipo = msg["tipo"]
        origem = msg["origem"]
        payload = msg["payload"]

        if tipo == "QUERY_REQ":
            sql = payload.get("sql", "").strip()
            print(f"[REQ] Query de {origem}: {sql[:50]}...")

            sql_upper = sql.upper()
            is_read = any(sql_upper.startswith(k) for k in ["SELECT", "SHOW", "DESCRIBE"])

            if is_read:
                # Leitura: Executa Local
                res = self.db.executar_query(sql)
                return self.criar_mensagem("QUERY_RESP", res)
            else:
                # Escrita
                if self.id == self.coordenador_id:
                    print(f"[MASTER] Executando e Replicando: {sql[:50]}...")
                    res = self.db.executar_query(sql)
                    
                    # Verifica erro antes de replicar
                    deu_erro = False
                    if isinstance(res, dict) and res.get("status") in ["ERRO", "ERROR"]: deu_erro = True
                    
                    if not deu_erro:
                        self.replicar_dados(sql)
                    else:
                        print("[MASTER] Erro local. Não replicando.")

                    return self.criar_mensagem("QUERY_RESP", res)
                else:
                    print(f"[SLAVE] Forwarding para Master {self.coordenador_id}")
                    resp = self.enviar_mensagem(self.coordenador_id, "QUERY_REQ", payload, esperar_resposta=True)
                    return resp if resp else self.criar_mensagem("ERRO", {"mensagem": "Master OFF"})

        elif tipo == "REPLICACAO":
            sql = payload.get("sql")
            print(f"[REPLICA] Gravando: {sql[:50]}...")
            self.db.executar_query(sql)
            return self.criar_mensagem("ACK")    

        elif tipo == "SYNC_REQ":
            print(f"[SYNC] Nó {origem} pediu dados. Gerando dump...")
            dump = self.db.get_full_dump() 
            return self.criar_mensagem("SYNC_DATA", dump)
        
        elif tipo == "SYNC_DATA":
            print(f"[SYNC] Recebi dados do Master.")
            if payload: self.aplicar_dump(payload)
            return None

        # Mensagens de controle simples (sem log excessivo)
        elif tipo == "HEARTBEAT": return self.criar_mensagem("VIVO")
        elif tipo == "QUEM_E_O_CHEFE":
            if self.id == self.coordenador_id: return self.criar_mensagem("EU_SOU_O_CHEFE")
        elif tipo == "COORDENADOR":
            self.coordenador_id = origem
            print(f"[INFO] Novo Master: {origem}")
        elif tipo == "ELEICAO":
             if int(self.id) > int(origem):
                threading.Thread(target=self.iniciar_eleicao).start()
                return self.criar_mensagem("VIVO")
        
        return None

    def replicar_dados(self, sql):
        for peer in self.peers:
            threading.Thread(target=self.enviar_mensagem, args=(peer, "REPLICACAO", {"sql": sql})).start()
            
    def iniciar_eleicao(self):
        print(f"[ELEIÇÃO] Iniciando...")
        sou_o_maior = True
        for peer in self.peers:
            if int(peer) > int(self.id):
                resp = self.enviar_mensagem(peer, "ELEICAO", esperar_resposta=True)
                if resp and resp["tipo"] == "VIVO":
                    sou_o_maior = False
                    break
        if sou_o_maior: self.tornar_coordenador()
    
    def tornar_coordenador(self):
        self.coordenador_id = self.id
        print(f"[MASTER] Assumindo Liderança!")
        for peer in self.peers: self.enviar_mensagem(peer, "COORDENADOR")
    
    def monitorar_coordenador(self):
        print("[MONITOR] Ativo.")
        while self.running:
            time.sleep(5)
            if self.id == self.coordenador_id: continue 
            resp = self.enviar_mensagem(self.coordenador_id, "HEARTBEAT", esperar_resposta=True)
            if not resp:
                print(f"[ALERTA] Master {self.coordenador_id} caiu!")
                self.iniciar_eleicao()
    
    def join_cluster(self):
        print("[JOIN] Entrando no cluster...")    
        coord = None
        for peer in self.peers:
            resp = self.enviar_mensagem(peer, "QUEM_E_O_CHEFE", esperar_resposta=True)
            if resp and resp["tipo"] == "EU_SOU_O_CHEFE":
                coord = peer
                break
        if coord:
            self.coordenador_id = coord
            print(f"[JOIN] Master encontrado: {coord}. Pedindo Sync...")
            self.enviar_mensagem(self.coordenador_id, "SYNC_REQ", esperar_resposta=True)
            # Obs: A resposta vem como uma nova msg SYNC_DATA processada no handle_client
        else:
            print("[JOIN] Sozinho na rede. Viro Master.")
            self.coordenador_id = self.id
  
    def run(self):
        threading.Thread(target=self.start_server, daemon=True).start()
        time.sleep(1)
        self.join_cluster()
        threading.Thread(target=self.monitorar_coordenador, daemon=True).start()
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            print("Encerrando.")

if __name__ == "__main__":
    if len(sys.argv) < 2: print("Uso: python middleware.py <ID_NO>")
    else: NodeMiddleware(sys.argv[1]).run()
