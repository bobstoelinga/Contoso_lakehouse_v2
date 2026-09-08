# Contoso Control Room

Streamlit-operatorconsole voor de bestaande Databricks audit/control plane.

## Lokaal starten

Installeer de dependencies uit `app/requirements.txt` en configureer:

- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`

Lokaal kan de app ook de bestaande Databricks CLI-profielauthenticatie
gebruiken. Dan zijn alleen de CLI-login en een draaiend SQL Warehouse nodig.

Start daarna:

```powershell
streamlit run app/streamlit_app.py
```

Voor Databricks Apps is `app/app.yaml` toegevoegd. Configureer daar de
secret-backed environment variables voor de SQL Warehouse-connectie en deploy
de map als Databricks App.

Een naamgevingsvoorbeeld staat in `.env.example`; zet tokens niet in Git.

De read-only monitoring gebruikt Databricks SQL. Operatoracties gebruiken de
Databricks SDK en vereisen job-ID's als environment variables, bijvoorbeeld
`CONTOSO_REQUEUE_DEAD_LETTER_WORK_ITEM_JOB_ID`.

Acties schrijven niet rechtstreeks naar audit-tabellen. Ze roepen de bestaande
remediation- en maintenance-jobs aan en vereisen reden, uitvoerder en
approval/change-referentie.