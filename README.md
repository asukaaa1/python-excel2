# Restaurant Dashboard - iFood API Version

Dashboard para gerenciamento de restaurantes integrado com a API do iFood.

## Instalação

1. Instalar dependências:
```bash
pip install -r requirements.txt
```

2. Configurar banco de dados PostgreSQL

3. Editar `ifood_config.json` com suas credenciais:
```json
{
  "client_id": "seu_client_id",
  "client_secret": "seu_client_secret",
  "merchants": [
    {"merchant_id": "id_merchant", "name": "Nome Restaurante", "manager": "Gerente"}
  ]
}
```

4. Iniciar servidor:
```bash
python dashboardserver.py
```

## Credenciais Padrão

- Admin: admin@dashboard.com / admin123
- User: user@dashboard.com / user123

## Estrutura

- `dashboardserver.py` - Servidor Flask
- `dashboarddb.py` - Módulo de banco de dados  
- `ifood_api.py` - Integração iFood API
- `ifood_config.json` - Configuração da API
- `dashboard_output/` - Arquivos HTML do frontend

## Como obter credenciais iFood

1. Acesse portal.ifood.com.br
2. Vá em Configurações > Integrações > API
3. Crie integração e copie Client ID/Secret
4. Anote os Merchant IDs dos restaurantes
