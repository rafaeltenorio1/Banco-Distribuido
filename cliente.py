import customtkinter as ctk
import socket
import json
import hashlib
import threading
import random
from datetime import datetime

# Configuração Visual
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

# Configuração de Rede
NODES = [
    {"ip": "localhost", "porta": 5001},
    {"ip": "localhost", "porta": 5002},
    {"ip": "localhost", "porta": 5003}
]

class ClientApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Terminal SQL Distribuído")
        self.geometry("900x700")
        
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        # 1. Cabeçalho
        self.header = ctk.CTkLabel(self, text="Terminal SQL - Banco Distribuído", font=("Roboto", 24, "bold"))
        self.header.grid(row=0, column=0, pady=15, sticky="ew")

        # 2. Área de Input SQL
        self.frame_input = ctk.CTkFrame(self)
        self.frame_input.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        
        self.lbl_instrucao = ctk.CTkLabel(self.frame_input, text="Digite sua Query SQL (CREATE, SELECT, INSERT, SHOW...):", anchor="w")
        self.lbl_instrucao.pack(fill="x", padx=10, pady=5)
        
        # Caixa de texto para digitar o SQL (aceita multilinhas)
        self.txt_sql = ctk.CTkTextbox(self.frame_input, height=100, font=("Consolas", 14))
        self.txt_sql.pack(fill="x", padx=10, pady=5)
        self.txt_sql.insert("0.0", "SELECT * FROM clientes;") # Texto padrão
        
        # Botões
        self.btn_run = ctk.CTkButton(self.frame_input, text="EXECUTAR QUERY", 
                                     fg_color="#0095C2", hover_color="#004696", 
                                     height=40, font=("Roboto", 14, "bold"),
                                     command=self.cmd_executar)
        self.btn_run.pack(fill="x", padx=10, pady=2)
        
        # Atalho de teclado para executar
        self.bind('<Control-Return>', lambda event: self.cmd_executar())

        # 3. Log / Resultados
        self.lbl_result = ctk.CTkLabel(self, text="Resultados / Logs:", anchor="w")
        self.lbl_result.grid(row=2, column=0, padx=20, sticky="w")

        self.log_box = ctk.CTkTextbox(self, font=("Consolas", 12), state="disabled")
        self.log_box.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="nsew")
        
        self.log_message("Sistema pronto. Conectado ao cluster.")

    def log_message(self, msg):
        self.log_box.configure(state="normal")
        time_str = datetime.now().strftime("%H:%M:%S")
        self.log_box.insert("end", f"[{time_str}] {msg}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def formatar_resultado(self, dados):
        """Formata JSON/Dict para uma string bonita de tabela"""
        if not dados:
            return "Nenhum dado retornado."
        
        if isinstance(dados, list) and len(dados) > 0:
            # Pega as chaves (colunas) do primeiro item
            colunas = list(dados[0].keys())
            header = " | ".join(colunas)
            divisor = "-" * len(header) * 2
            
            linhas = [header, divisor]
            for row in dados:
                valores = [str(row[c]) for c in colunas]
                linhas.append(" | ".join(valores))
            
            return "\n".join(linhas)
        else:
            return str(dados)

    def criar_mensagem(self, tipo, payload):
        payload_str = json.dumps(payload, sort_keys=True)
        checksum = hashlib.md5(payload_str.encode("utf-8")).hexdigest()
        return {
            "tipo": tipo,
            "origem": "CLIENT_GUI",
            "payload": payload,
            "checksum": checksum
        }

    def enviar_rede(self, sql):
        def thread_task():
            node = random.choice(NODES)
            self.log_message(f"Enviando para Nó {node['porta']}...")
            
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10) # Timeout maior para queries pesadas
                sock.connect((node['ip'], node['porta']))
                
                msg = self.criar_mensagem("QUERY_REQ", {"sql": sql})
                sock.sendall(json.dumps(msg).encode("utf-8"))
                
                resp_bytes = sock.recv(65536) # Buffer grande
                sock.close()
                
                if resp_bytes:
                    resp = json.loads(resp_bytes.decode("utf-8"))
                    payload = resp.get("payload", {})
                    
                    if resp.get("tipo") == "QUERY_RESP":
                        status = payload.get("status", "UNKNOWN")
                        self.log_message(f"Status: {status}")
                        
                        if "dados" in payload:
                            tabela_formatada = self.formatar_resultado(payload["dados"])
                            self.log_message(f"\n{tabela_formatada}\n")
                        elif "mensagem" in payload:
                            self.log_message(f"Resposta: {payload['mensagem']}")
                    else:
                        self.log_message(f"Erro remoto: {payload}")
                else:
                    self.log_message("Sem resposta do servidor (Timeout ou erro).")

            except Exception as e:
                self.log_message(f"Erro na conexão: {e}")

        threading.Thread(target=thread_task, daemon=True).start()

    def cmd_executar(self):
        # Pega o texto da caixa (do inicio "1.0" até o final "end")
        sql = self.txt_sql.get("1.0", "end").strip()
        
        if not sql:
            self.log_message("Digite uma query para executar.")
            return
        
        self.log_message(f"Executando: {sql}")
        self.enviar_rede(sql)

if __name__ == "__main__":
    app = ClientApp()
    app.mainloop()