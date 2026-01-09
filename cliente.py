import socket
import json
import random
import hashlib
import threading
import customtkinter as ctk
from datetime import datetime

# --- CONFIGURAÇÕES DO SISTEMA ---
ctk.set_appearance_mode("Dark")  # Modo escuro (Alto Contraste)
ctk.set_default_color_theme("dark-blue")  # Tema de cores azul/ciano

NODES = [
    {"ip": "192.168.15.6", "port": 5001},
    {"ip": "192.168.15.48", "port": 5001}
]

def calcular_checksum(payload):
    dump = json.dumps(payload, sort_keys=True).encode()
    return hashlib.md5(dump).hexdigest()

class CupuacuClient(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Configuração da Janela Principal
        self.title("Cupuaçu DB - Cliente Distribuído")
        self.geometry("700x650")
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1) # A área de log expande

        # --- CABEÇALHO ---
        self.lbl_title = ctk.CTkLabel(self, text="DASHBOARD DO CLIENTE", 
                                      font=("Roboto Medium", 20),
                                      text_color="#4CB5F5") # Azul claro para contraste
        self.lbl_title.grid(row=0, column=0, pady=(20, 10), sticky="ew")

        # --- FRAME DE INSERÇÃO (CARD) ---
        self.frame_inputs = ctk.CTkFrame(self, corner_radius=15)
        self.frame_inputs.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.frame_inputs.grid_columnconfigure(1, weight=1)

        # Inputs
        ctk.CTkLabel(self.frame_inputs, text="Nome Completo:", font=("Roboto", 12)).grid(row=0, column=0, padx=15, pady=(15, 5), sticky="w")
        self.entry_nome = ctk.CTkEntry(self.frame_inputs, placeholder_text="Ex: Roberto Silva", height=35)
        self.entry_nome.grid(row=0, column=1, padx=15, pady=(15, 5), sticky="ew")

        ctk.CTkLabel(self.frame_inputs, text="E-mail:", font=("Roboto", 12)).grid(row=1, column=0, padx=15, pady=5, sticky="w")
        self.entry_email = ctk.CTkEntry(self.frame_inputs, placeholder_text="Ex: roberto@email.com", height=35)
        self.entry_email.grid(row=1, column=1, padx=15, pady=5, sticky="ew")

        # Botões de Ação (Grid layout dentro do frame)
        self.btn_insert = ctk.CTkButton(self.frame_inputs, text="GRAVAR DADOS (INSERT)", 
                                        fg_color="#2CC985", hover_color="#229A65", # Verde vibrante
                                        height=40, font=("Roboto", 12, "bold"),
                                        command=lambda: self.run_async(self.fazer_insert))
        self.btn_insert.grid(row=2, column=0, columnspan=2, padx=15, pady=(15, 10), sticky="ew")

        self.btn_select = ctk.CTkButton(self.frame_inputs, text="CONSULTAR BANCO (SELECT)", 
                                        fg_color="#3B8ED0", hover_color="#1F6AA5", # Azul padrão
                                        height=40, font=("Roboto", 12, "bold"),
                                        command=lambda: self.run_async(self.fazer_select))
        self.btn_select.grid(row=3, column=0, columnspan=2, padx=15, pady=(0, 15), sticky="ew")

        # --- ÁREA DE LOG (TERMINAL) ---
        self.lbl_log = ctk.CTkLabel(self, text="Terminal de Respostas", anchor="w", text_color="gray")
        self.lbl_log.grid(row=2, column=0, padx=25, pady=(10,0), sticky="w")

        self.txt_log = ctk.CTkTextbox(self, font=("Consolas", 13), activate_scrollbars=True)
        self.txt_log.grid(row=3, column=0, padx=20, pady=(5, 20), sticky="nsew")
        self.txt_log.configure(state="disabled") # Somente leitura

        # Tags de cor manual (Workaround para CTkTextbox)
        self.log_message("Sistema iniciado. Pronto para conexão.", "info")

    def log_message(self, message, type="info"):
        """Escreve no log com cores baseadas no tipo"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Definição de cores (Simulação visual via texto formatado)
        prefix = f"[{timestamp}] "
        full_msg = f"{prefix} {message}\n"
        
        self.txt_log.configure(state="normal")
        self.txt_log.insert("end", full_msg)
        self.txt_log.configure(state="disabled")
        self.txt_log.see("end")

    def run_async(self, func):
        """Executa funções de rede em uma thread separada para não travar a GUI"""
        threading.Thread(target=func, daemon=True).start()

    def enviar_query(self, sql):
        node = random.choice(NODES)
        self.log_message(f"Conectando a {node['ip']}...", "info")

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3) # Timeout rápido
            s.connect((node['ip'], node['port']))
            
            payload = {"sql": sql}
            msg = {
                "tipo": "QUERY_REQ",
                "origem": "GUI_CLIENT",
                "payload": payload,
                "checksum": calcular_checksum(payload)
            }
            
            s.send(json.dumps(msg).encode())
            resp_raw = s.recv(4096).decode()
            s.close()

            response = json.loads(resp_raw)
            self.processar_resposta(response, node['port'])

        except Exception as e:
            self.log_message(f"FALHA: Não foi possível conectar ao nó {node['port']}", "error")
            self.log_message(f"Erro técnico: {str(e)}", "error")

    def processar_resposta(self, response, port):
        status = response.get('result', {}).get('status')
        node_exec = response.get('node_exec')
        
        msg_header = f"RESPOSTA RECEBIDA (Nó {node_exec}): Status [{status}]"
        self.log_message(msg_header)

        if 'dados' in response.get('result', {}):
            self.log_message("--- DADOS ---")
            for linha in response['result']['dados']:
                self.log_message(f" > {linha}")
            self.log_message("-------------")
        
        if response.get('error'):
            self.log_message(f"ERRO SQL: {response['error']}", "error")

    def fazer_insert(self):
        nome = self.entry_nome.get()
        email = self.entry_email.get()
        
        if not nome or not email:
            self.log_message("Validação: Preencha todos os campos!", "error")
            return

        sql = f"INSERT INTO clientes (nome, email) VALUES ('{nome}', '{email}')"
        self.enviar_query(sql)
        
        # Limpa os campos na thread principal (necessário cuidado com threads, mas CTk aguenta)
        self.entry_nome.delete(0, "end")
        self.entry_email.delete(0, "end")

    def fazer_select(self):
        self.enviar_query("SELECT * FROM clientes")

if __name__ == "__main__":
    app = CupuacuClient()
    app.mainloop()
