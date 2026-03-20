# sienge-sync

Job de sincronização Sienge → PostgreSQL rodando na VPS Hostinger.

## Estrutura

```
sienge-sync/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .env                  ← você cria a partir do .env.example (não commitar)
├── logs/                 ← gerado automaticamente
└── scripts/
    ├── run_all.py        ← orquestrador (chamado pelo cron)
    ├── db_utils.py       ← utilitários compartilhados de banco
    ├── contas_receber.py
    ├── contas_receber_lot.py
    ├── contas_recebidas.py
    └── contas_recebidas_lot.py
```

## Instalação na VPS

### 1. Instalar Docker

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# reconecte ao SSH para o grupo ter efeito
```

### 2. Clonar / copiar os arquivos

```bash
mkdir -p ~/sienge-sync
# copie os arquivos para ~/sienge-sync via scp ou sftp
```

### 3. Criar o .env

```bash
cd ~/sienge-sync
cp .env.example .env
nano .env   # preencha com suas credenciais reais
```

### 4. Subir o banco PostgreSQL

```bash
docker compose up -d postgres
```

Verifique se subiu:

```bash
docker compose ps
docker compose logs postgres
```

### 5. Testar o job manualmente

```bash
docker compose run --rm sync-job
```

Acompanhe os logs em tempo real:

```bash
docker compose logs -f sync-job
```

Os logs também ficam salvos em `./logs/`.

### 6. Configurar o cron

```bash
crontab -e
```

Adicione a linha (roda todo dia às 02h00):

```
0 2 * * * cd /root/sienge-sync && docker compose run --rm sync-job >> /root/sienge-sync/logs/cron.log 2>&1
```

Verifique se o cron está ativo:

```bash
crontab -l
```

---

## Conectar na VPS via SSH Tunnel (VM local / Power BI)

Na máquina local, abra o tunnel:

```bash
ssh -L 5433:localhost:5432 usuario@IP_DA_VPS -N
```

A porta 5433 local passa a apontar para o PostgreSQL da VPS.
Conecte com qualquer client usando `localhost:5433`.

Para automatizar no script Python local:

```python
from sshtunnel import SSHTunnelForwarder
import psycopg2

with SSHTunnelForwarder(
    'IP_DA_VPS',
    ssh_username='root',
    ssh_pkey='/caminho/para/chave.pem',
    remote_bind_address=('localhost', 5432),
    local_bind_address=('localhost', 5433)
) as tunnel:
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='sienge_central',
        user='sienge_user', password='SENHA'
    )
    # faça suas queries aqui
```

---

## Monitoramento

Ver logs do último job:

```bash
ls -lt ~/sienge-sync/logs/ | head -10
cat ~/sienge-sync/logs/cron.log
```

Ver uso de recursos dos containers:

```bash
docker stats
```

---

## Atualização dos scripts

```bash
cd ~/sienge-sync
# edite os arquivos em scripts/
docker compose build sync-job   # rebuilda a imagem
docker compose run --rm sync-job  # testa
```

---

## Limites de recurso configurados

| Container | CPU máx | Memória máx |
|---|---|---|
| postgres | 1 núcleo | 2 GB |
| sync-job | 2 núcleos | 4 GB |
| **Total** | **3 núcleos** | **6 GB** |

Deixa folga confortável para os outros containers que já rodam na VPS.
