# Connettore FTP Non Cumulativo - Dal Ben Abbigliamento

Sistema ETL per la sincronizzazione dati tra server FTP e database PostgreSQL su Google Cloud Platform.

## Descrizione

Il connettore scarica automaticamente i file CSV più recenti dalle directory FTP configurate e li carica nel database PostgreSQL, eseguendo un refresh completo dei dati per ogni tabella. Il sistema gestisce diverse categorie di dati:

- **Anagrafiche**: brands, collections, locations, payment methods, suppliers, etc.
- **Clienti**: clients, cards, destinations, privacy, references
- **Prodotti**: products, attributes, barcodes, classifications, variants
- **Magazzino**: stock data
- **Vendite**: sale profiles and shops

## Deployment

Il progetto è configurato come Google Cloud Function e viene attivato tramite richieste HTTP.

## Configurazione Variabili d'Ambiente

Il sistema richiede le seguenti variabili d'ambiente:

### Database PostgreSQL
```
host=<hostname_database>
port=<porta_database>
dbname=<nome_database>
user=<utente_database>
password=<password_database>
```

### Server FTP
```
ftp_host=<hostname_ftp>
ftp_username=<utente_ftp>
ftp_password=<password_ftp>
```

## Funzionalità

1. **Download automatico**: Scarica l'ultimo file CSV da ogni directory FTP
2. **Creazione schema/tabelle**: Crea automaticamente schemi e tabelle se non esistono
3. **Refresh completo**: Svuota e ricarica completamente ogni tabella
4. **Esecuzione funzioni**: Esegue le funzioni PostgreSQL `aggiorna_stock_export_info()` e `esegui_riassortimento()`
5. **Logging completo**: Traccia tempi di esecuzione e errori

## Struttura Dati

Il mapping FTP-Database è definito nella costante `FTP_DB_MAP` che associa:
- Directory FTP → Schema.Tabella PostgreSQL

## Sicurezza

- Tutte le credenziali sono gestite tramite variabili d'ambiente
- Connessioni sicure al database PostgreSQL
- Gestione automatica delle connessioni e cleanup delle risorse

## Monitoraggio

Il sistema fornisce output dettagliato con:
- Status di ogni operazione
- Tempi di esecuzione per tabella
- Tempo totale di elaborazione
- Gestione errori con logging specifico